package net.java21.data2flow.action.actuation.driver.virtual;

import net.java21.data2flow.action.actuation.driver.DeviceDriver;
import net.java21.data2flow.action.actuation.driver.DriverCommand;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverDevice;
import net.java21.data2flow.action.actuation.driver.DriverHealth;
import net.java21.data2flow.action.actuation.driver.DriverResult;
import net.java21.data2flow.action.actuation.driver.ReportedState;
import net.java21.data2flow.action.actuation.driver.StateListener;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Virtual 드라이버(ACT-03.02, SIM-03.02): 시뮬레이터 가상 장비를 내부 API로 제어한다.
 *
 * <ul>
 *   <li>명령: {@code POST /internal/sim/devices/{device-id}/commands {commandId, capability, command, args, desiredVersion}} → 202.
 *       응답(ack)·상태 보고는 시뮬레이터가 {@code data2flow.events}로 낸다(EVT-SIM-03 = EVT-ACT-06·07). 같은 commandId는 한 번만 적용된다.</li>
 *   <li>상태: {@code GET /internal/sim/devices/{device-id}/state}(API-SIM-32)</li>
 *   <li>오류 매핑: 400 → FAILED(INVALID_COMMAND), 404 → FAILED(DEVICE_NOT_SIMULATED), 5xx·연결 실패 → FAILED(DRIVER_ERROR, 재시도)</li>
 * </ul>
 * 서비스 간 호출에는 토큰을 쓰지 않고 {@code X-CALLER-SERVICE}만 붙인다(ADR-021).
 */
public class VirtualDriver implements DeviceDriver {

    public static final String TYPE = "VIRTUAL";
    static final Set<String> CAPABILITIES = Set.of("Switch", "Thermostat", "FanSpeed", "Ventilation", "Dimmer", "Lock", "Contact");
    private static final Logger log = LoggerFactory.getLogger(VirtualDriver.class);

    private final RestClient simulator;
    private final Clock clock;

    public VirtualDriver(RestClient simulator, Clock clock) {
        this.simulator = simulator;
        this.clock = clock;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public DriverHealth healthCheck(DriverConfig config) {
        Instant start = clock.instant();
        try {
            Integer status = simulator.get().uri("/internal/sim/catalog")
                    .header(DataflowHeaders.CALLER_SERVICE, "data2flow-action")
                    .exchange((req, res) -> res.getStatusCode().value());
            long ms = elapsed(start);
            if (status != null && status < 500) {
                return DriverHealth.up(ms, CAPABILITIES);
            }
            return DriverHealth.down(ms, CAPABILITIES, "UNAVAILABLE", "simulator HTTP " + status);
        } catch (RuntimeException e) {
            return DriverHealth.down(elapsed(start), CAPABILITIES, "UNREACHABLE", e.getMessage());
        }
    }

    @Override
    public DriverResult execute(DriverCommand command) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("commandId", command.commandId().toString());
        body.put("capability", command.capability());
        body.put("command", command.command());
        body.put("args", command.args());
        body.put("desiredVersion", command.desiredVersion());
        try {
            return simulator.post().uri("/internal/sim/devices/{id}/commands", command.device().deviceId())
                    .header(DataflowHeaders.CALLER_SERVICE, "data2flow-action")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .exchange((req, res) -> map(res.getStatusCode().value()));
        } catch (RuntimeException e) {
            log.warn("가상 장비 명령 전달 실패 device={} command={}: {}", command.device().deviceId(), command.commandId(), e.toString());
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, e.getMessage());
        }
    }

    private static DriverResult map(int status) {
        if (status >= 200 && status < 300) {
            return DriverResult.accepted();
        }
        if (status == 404) {
            return DriverResult.failed(CommandStatusReasons.DEVICE_NOT_SIMULATED, false, "simulator 404");
        }
        if (status >= 400 && status < 500) {
            return DriverResult.failed(CommandStatusReasons.INVALID_COMMAND, false, "simulator " + status);
        }
        return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, "simulator " + status);
    }

    @Override
    public Optional<ReportedState> getState(DriverDevice device) {
        try {
            JsonNode body = simulator.get().uri("/internal/sim/devices/{id}/state", device.deviceId())
                    .header(DataflowHeaders.CALLER_SERVICE, "data2flow-action")
                    .exchange((req, res) -> res.getStatusCode().is2xxSuccessful() ? res.bodyTo(JsonNode.class) : null);
            if (body == null) {
                return Optional.empty();
            }
            JsonNode r = body.path("response");
            Map<String, Map<String, Object>> caps = new LinkedHashMap<>();
            r.path("capabilities").properties().forEach(e -> {
                Map<String, Object> attrs = new LinkedHashMap<>();
                e.getValue().properties().forEach(a -> attrs.put(a.getKey(), value(a.getValue())));
                caps.put(e.getKey(), attrs);
            });
            Instant at = r.hasNonNull("reportedAt") ? Instant.parse(r.get("reportedAt").asString()) : clock.instant();
            return Optional.of(new ReportedState(r.path("externalId").asString(null), caps, r.path("version").asLong(0), at));
        } catch (RuntimeException e) {
            log.warn("가상 장비 상태 조회 실패 device={}: {}", device.deviceId(), e.toString());
            return Optional.empty();
        }
    }

    private static Object value(JsonNode n) {
        if (n.isBoolean()) {
            return n.booleanValue();
        }
        if (n.isIntegralNumber()) {
            return n.longValue();
        }
        if (n.isNumber()) {
            return n.doubleValue();
        }
        return n.isNull() ? null : n.asString();
    }

    /** 가상 장비는 상태를 이벤트(EVT-SIM-03)로 직접 보고하므로 따로 구독하지 않는다 */
    @Override
    public void subscribeState(DriverDevice device, StateListener listener) {
        // push는 data2flow.events device.state.reported로 들어온다
    }

    private long elapsed(Instant start) {
        return Math.max(0, clock.instant().toEpochMilli() - start.toEpochMilli());
    }
}
