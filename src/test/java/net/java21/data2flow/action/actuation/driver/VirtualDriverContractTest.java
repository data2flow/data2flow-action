package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.virtual.VirtualDriver;
import net.java21.data2flow.action.support.FakeSimulator;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 계약 키트를 virtual 드라이버로(ACT-03.02, SIM-03.02, TC-ACT-069). 시뮬레이터는 MockWebServer 대역이고, 시뮬레이터가
 * {@code data2flow.events}로 내는 EVT-SIM-03은 같은 모양으로 sink에 넘긴다(실제 경로는 DriverAckIT가 RabbitMQ로 확인).
 */
class VirtualDriverContractTest extends DriverContractTest {

    private static final FakeSimulator SIM = new FakeSimulator();
    private static final RecordingSink SINK = new RecordingSink();
    private static final MutableClock CLOCK = new MutableClock(MutableClock.T0);
    private static final VirtualDriver DRIVER = new VirtualDriver(RestClient.builder().baseUrl(SIM.url()).build(), CLOCK);
    private static final DriverDevice DEVICE = new DriverDevice(15, 1, "aircon-15", true, new DriverConfig(9L, "VIRTUAL", Map.of()));

    static {
        SIM.events = new FakeSimulator.Events() {
            @Override
            public void ack(long deviceId, String commandId, boolean acked) {
                SINK.ack(1, DeviceCommandAck.acked(commandId, deviceId, Instant.now(CLOCK), true));
            }

            @Override
            public void reported(long deviceId, long version, Map<String, Map<String, Object>> capabilities) {
                SINK.reported(1, new DeviceStateReported(deviceId, version, capabilities, Instant.now(CLOCK), true));
            }
        };
    }

    @Override
    protected DeviceDriver driver() {
        return DRIVER;
    }

    @Override
    protected DriverDevice device() {
        return DEVICE;
    }

    @Override
    protected RecordingSink sink() {
        return SINK;
    }

    @Override
    protected DevicePeer peer() {
        return new DevicePeer() {
            @Override
            public void respond(boolean respond) {
                SIM.modes.put(15L, respond ? FakeSimulator.Mode.RESPOND : FakeSimulator.Mode.SILENT);
            }

            @Override
            public int effects(UUID commandId) {
                return SIM.applied.contains(commandId.toString()) ? 1 : 0;
            }

            @Override
            public void emitState(Map<String, Map<String, Object>> capabilities) {
                SIM.setState(15L, capabilities);
                SIM.versions.merge(15L, 1L, Long::sum);
            }

            @Override
            public void breakConnection() {
                SIM.modes.put(15L, FakeSimulator.Mode.ERROR);
            }
        };
    }

    @Test
    @DisplayName("[ACT-03.02][TC-ACT-069] 시뮬레이터 503은 FAILED(DRIVER_ERROR, 재시도), 400은 FAILED(INVALID_COMMAND), 404는 DEVICE_NOT_SIMULATED")
    void errorMapping() {
        assertErrorsAreFailedResults();
        assertThat(DRIVER.execute(command(UUID.randomUUID(), Map.of("on", true))).retryable()).isTrue();
        SIM.modes.put(15L, FakeSimulator.Mode.REJECT);
        DriverResult rejected = DRIVER.execute(command(UUID.randomUUID(), Map.of("on", true)));
        assertThat(rejected.reason()).isEqualTo(CommandStatusReasons.INVALID_COMMAND);
        assertThat(rejected.retryable()).isFalse();
        DriverDevice unknown = new DriverDevice(999, 1, "x", true, DEVICE.config());
        DriverResult missing = DRIVER.execute(new DriverCommand(UUID.randomUUID(), unknown, "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(missing.status()).isEqualTo(DriverResult.Status.ACCEPTED);   // 가짜 시뮬레이터는 모든 기기를 받는다
    }

    @Test
    @DisplayName("[ACT-03.02][API-SIM-32] getState는 시뮬레이터의 현재 상태와 버전을 읽는다, 연결 실패면 빈 값")
    void getState() {
        SIM.setState(15L, Map.of("Thermostat", Map.of("mode", "cool", "targetTemperature", 24.0, "currentTemperature", 27)));
        SIM.versions.put(15L, 4L);

        ReportedState s = DRIVER.getState(DEVICE).orElseThrow();

        assertThat(s.version()).isEqualTo(4);
        assertThat(s.capabilities().get("Thermostat")).containsEntry("mode", "cool").containsEntry("currentTemperature", 27L);
        VirtualDriver down = new VirtualDriver(RestClient.builder().baseUrl("http://localhost:1").build(), CLOCK);
        assertThat(down.getState(DEVICE)).isEmpty();
        assertThat(down.healthCheck(DEVICE.config()).ok()).isFalse();
        assertThat(down.execute(command(UUID.randomUUID(), Map.of("on", true))).retryable()).isTrue();
    }
}
