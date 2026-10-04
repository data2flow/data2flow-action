package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.output.domain.DeviceContext;
import net.java21.data2flow.action.output.domain.OutboundMessage;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.action.output.domain.OutputRenderer;
import net.java21.data2flow.action.output.repository.OutputDeliveryRepository;
import net.java21.data2flow.action.output.repository.OutputDeliveryRepository.NewDelivery;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 텔레메트리 묶음을 출력 연결마다 걸러 대기열에 쌓는다(DSC-04.01). 소비자는 이 메서드가 커밋된 뒤에만 스트림 오프셋을 저장한다
 * (유실 0). 같은 메시지를 다시 받아도 (연결, 메시지, 부분) 키로 한 번만 쌓인다. 실제 발송은 {@link OutputDeliveryService}가 따로 하므로
 * 대상 장애가 수집·소비를 늦추지 않는다(BR-DSC-19).
 */
public class OutputEnqueueService {

    private static final Logger log = LoggerFactory.getLogger(OutputEnqueueService.class);

    private final OutputConnectionRegistry registry;
    private final DeviceContextCache contexts;
    private final OutputDeliveryRepository deliveries;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final String env;

    public OutputEnqueueService(OutputConnectionRegistry registry, DeviceContextCache contexts, OutputDeliveryRepository deliveries,
                                TransactionTemplate tx, Clock clock, String env) {
        this.registry = registry;
        this.contexts = contexts;
        this.deliveries = deliveries;
        this.tx = tx;
        this.clock = clock;
        this.env = env;
    }

    /** 쌓은 행 수. core에서 기기 맥락을 못 받으면 예외(오프셋을 저장하지 않고 다시 시도) */
    public int enqueue(List<CanonicalTelemetry> batch) {
        registry.refreshIfStale();
        Map<Long, Long> devices = new HashMap<>();
        for (CanonicalTelemetry t : batch) {
            if (registry.hasActive(t.organizationId())) {
                devices.put(t.deviceId(), t.organizationId());
            }
        }
        if (devices.isEmpty()) {
            return 0;
        }
        Map<Long, DeviceContext> ctx = contexts.get(devices);
        List<NewDelivery> rows = new ArrayList<>();
        for (CanonicalTelemetry t : batch) {
            if (!devices.containsKey(t.deviceId())) {
                continue;
            }
            DeviceContext c = ctx.getOrDefault(t.deviceId(), DeviceContext.unknown(t.deviceId(), t.organizationId()));
            for (OutputConnection conn : registry.active(t.organizationId())) {
                try {
                    for (OutboundMessage m : OutputRenderer.render(conn, t, c)) {
                        rows.add(new NewDelivery(t.organizationId(), conn.id(), t.messageId(), m.part(), t.deviceId(), t.measuredAt(),
                                m.topic(), m.body()));
                    }
                } catch (IllegalArgumentException e) {
                    log.warn("출력 연결 {} 정의로 메시지를 만들 수 없습니다(건너뜀): {}", conn.id(), e.getMessage());
                }
            }
        }
        if (rows.isEmpty()) {
            return 0;
        }
        Integer inserted = tx.execute(s -> {
            int n = 0;
            for (NewDelivery d : rows) {
                n += deliveries.insert(d.organizationId(), d, env, clock.instant());
            }
            return n;
        });
        return inserted == null ? 0 : inserted;
    }
}
