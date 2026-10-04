package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.CircuitPolicy;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker;
import net.java21.data2flow.action.actuation.repository.DriverCallRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DriverCircuitChanged;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 드라이버 상태(ACT-03.06)와 서킷 브레이커(ACT-07.03, BR-ACT-14). 호출마다 {@code driver_calls}에 기록하고, 1분 실패율이 기준을 넘으면 서킷을
 * 열어 30초 동안 즉시 FAILED(DRIVER_UNAVAILABLE)로 끝낸다. 열림·닫힘은 EVT-ACT-05 {@code driver.circuit.opened|closed}로 알리고
 * core가 운영 알람(MAJOR)을 만든다. 지표(API-ACT-32)는 같은 기록에서 계산한다.
 */
public class DriverHealthService {

    private static final Logger log = LoggerFactory.getLogger(DriverHealthService.class);

    private final DriverCallRepository calls;
    private final OutboxWriter outbox;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final Clock clock;

    public DriverHealthService(DriverCallRepository calls, OutboxWriter outbox, PlatformTransactionManager txManager, MeterRegistry meters,
                               Clock clock) {
        this.calls = calls;
        this.outbox = outbox;
        this.tx = new TransactionTemplate(txManager);
        this.meters = meters;
        this.clock = clock;
    }

    /** 호출 전 판정. TRIAL이면 상태를 HALF_OPEN으로 바꾸고 시험 호출을 보낸다 */
    public DriverCircuitBreaker.Admission admit(long organizationId, DriverBinding binding) {
        if (binding.driverId() == null) {
            return DriverCircuitBreaker.Admission.ALLOW;
        }
        return tx.execute(s -> {
            Instant now = clock.instant();
            DriverCircuitBreaker.Circuit c = calls.lockCircuit(binding.driverId(), organizationId, binding.type(), now);
            DriverCircuitBreaker.Admission a = DriverCircuitBreaker.admit(c, binding.circuit(), now);
            if (a == DriverCircuitBreaker.Admission.TRIAL) {
                calls.saveCircuit(binding.driverId(), new DriverCircuitBreaker.Circuit(DriverCircuitBreaker.State.HALF_OPEN, c.openedAt(), now),
                        null, now);
            }
            return a;
        });
    }

    /** 호출 결과를 기록하고 서킷 상태를 바꾼다(열림·닫힘이면 EVT-ACT-05) */
    public void record(long organizationId, DriverBinding binding, UUID commandId, boolean ok, long latencyMs, String error) {
        if (binding.driverId() == null) {
            return;
        }
        meters.counter("data2flow_action_driver_requests_total", "type", binding.type(), "result", ok ? "ok" : "error").increment();
        tx.executeWithoutResult(s -> {
            Instant now = clock.instant();
            calls.insert(organizationId, binding.driverId(), binding.type(), now, ok, latencyMs, commandId, error);
            CircuitPolicy policy = binding.circuit();
            DriverCircuitBreaker.Circuit before = calls.lockCircuit(binding.driverId(), organizationId, binding.type(), now);
            int[] window = calls.countSince(binding.driverId(), now.minus(policy.window()));
            DriverCircuitBreaker.Circuit after = DriverCircuitBreaker.after(before, policy, ok, window[0], window[1], now);
            double rate = DriverCircuitBreaker.failureRate(window[0], window[1]);
            if (after.equals(before)) {
                return;
            }
            calls.saveCircuit(binding.driverId(), after, rate, now);
            if (after.state() == DriverCircuitBreaker.State.OPEN && before.state() != DriverCircuitBreaker.State.OPEN) {
                if (before.state() == DriverCircuitBreaker.State.CLOSED) {
                    log.warn("드라이버 서킷 열림 driver={} type={} 실패율={}", binding.driverId(), binding.type(), rate);
                    outbox.event(EventType.DRIVER_CIRCUIT_OPENED, organizationId,
                            new DriverCircuitChanged(binding.driverId(), binding.type(), Math.min(1, rate), now),
                            "circuit:" + binding.driverId() + ":open:" + now.toEpochMilli());
                    meters.counter("data2flow_action_driver_circuit_opened_total", "type", binding.type()).increment();
                }
            } else if (after.state() == DriverCircuitBreaker.State.CLOSED) {
                outbox.event(EventType.DRIVER_CIRCUIT_CLOSED, organizationId,
                        new DriverCircuitChanged(binding.driverId(), binding.type(), Math.min(1, rate), now),
                        "circuit:" + binding.driverId() + ":closed:" + now.toEpochMilli());
            }
        });
    }

    /**
     * 지표(API-ACT-32 {@code GET /drivers/{driver-id}/metrics?window=1h|24h}).
     */
    public Map<String, Object> metrics(long organizationId, long driverId, Duration window) {
        Instant now = clock.instant();
        DriverCallRepository.Stats st = calls.stats(organizationId, driverId, now.minus(window), now);
        var circuit = calls.findCircuit(organizationId, driverId);
        String circuitState = circuit.map(DriverCallRepository.CircuitRow::state).orElse("CLOSED");
        double errorRate = st.requests() == 0 ? 0 : (double) st.errors() / st.requests();
        String status;
        if (!"CLOSED".equals(circuitState)) {
            status = "CIRCUIT_OPEN";
        } else if (st.requests() == 0) {
            status = "UNTESTED";
        } else if (errorRate > 0.5) {
            status = "ERROR";
        } else {
            status = "OK";
        }
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("state", circuitState);
        circuit.map(DriverCallRepository.CircuitRow::openedAt).ifPresent(t -> c.put("openedAt", t));
        List<Map<String, Object>> recent = calls.findRecentErrors(organizationId, driverId, now.minus(window), 10).stream().map(e -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("at", e.at());
            m.put("commandId", e.commandId() == null ? null : e.commandId().toString());
            m.put("message", e.message());
            return m;
        }).toList();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", status);
        out.put("circuit", c);
        out.put("requests", st.requests());
        out.put("errors", st.errors());
        out.put("errorRate", Math.round(errorRate * 10000) / 10000.0);
        out.put("avgMs", Math.round(st.avgMs() * 10) / 10.0);
        out.put("p95Ms", Math.round(st.p95Ms() * 10) / 10.0);
        out.put("recentErrors", recent);
        return out;
    }

    /** 2일보다 오래된 호출 기록을 지운다 */
    public int purge() {
        return calls.deleteBefore(clock.instant().minus(Duration.ofDays(2)));
    }
}
