package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.FakeSimulator;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * UC-ACT-16 드라이버 응답 처리: {@code action.events}로 받은 {@code device.command.ack}·{@code device.state.reported}(BR-ACT-25).
 * ACT-02.02·02.04·03.02
 */
class DriverAckIT extends IntegrationTestSupport {

    private String commandId;

    @BeforeEach
    void sent() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        SIM.modes.put(Fixtures.AIRCON, FakeSimulator.Mode.SILENT);
        Result r = post(api(Fixtures.USER, Fixtures.ORG), "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat",
                "command", "set", "args", Map.of("mode", "cool", "targetTemperature", 24)), "ack-1");
        commandId = r.response().path("id").asString();
        assertThat(status(commandId)).isEqualTo("SENT");
        assertThat(SIM.received.get(0).path("desiredVersion").asLong()).isEqualTo(1);
    }

    @Test
    @DisplayName("[ACT-02.02][AT-ACT-16.1][TC-ACT-035] device.command.ack ACKED → 상태 ACKED, acked_at 기록")
    void acked() {
        publish(EventType.DEVICE_COMMAND_ACK, DeviceCommandAck.acked(commandId, Fixtures.AIRCON, clock.instant(), true));

        await().atMost(Duration.ofSeconds(5)).until(() -> status(commandId).equals("ACKED"));
        assertThat(count("SELECT count(*) FROM data2flow_action.commands WHERE acked_at IS NOT NULL")).isEqualTo(1);
        // 적용 기한(60초) 안에 보고가 없으면 TIMEOUT_APPLY
        clock.advanceBy(Duration.ofSeconds(61));
        tracker.processDue();
        assertThat(status(commandId)).isEqualTo("TIMEOUT");
    }

    @Test
    @DisplayName("[ACT-02.04][AT-ACT-16.2][TC-ACT-036] device.state.reported v8 mode=cool → APPLIED, device.state.changed 1회")
    void applied() {
        publish(EventType.DEVICE_COMMAND_ACK, DeviceCommandAck.acked(commandId, Fixtures.AIRCON, clock.instant(), true));
        await().atMost(Duration.ofSeconds(5)).until(() -> status(commandId).equals("ACKED"));
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(Fixtures.AIRCON, 8,
                Map.of("Thermostat", Map.of("mode", "cool", "targetTemperature", 24.0, "currentTemperature", 27.4)), clock.instant(), true));

        await().atMost(Duration.ofSeconds(5)).until(() -> status(commandId).equals("APPLIED"));
        List<JsonNode> changed = events("device.state.changed");
        assertThat(changed).hasSize(1);
        assertThat(changed.get(0).path("payload").path("origin").asString()).isEqualTo("COMMAND");
        assertThat(changed.get(0).path("payload").path("reportedVersion").asLong()).isEqualTo(8);
        assertThat(events("command.status.applied").get(0).path("payload").path("spaceId").asLong()).isEqualTo(Fixtures.SPACE);
        // 상태 구간(TSD-01.03): mode·targetTemperature·currentTemperature 열린 구간
        assertThat(count("SELECT count(*) FROM data2flow_action.device_state_history WHERE valid_to IS NULL")).isEqualTo(3);
        // 보호 상태: 켜짐 기록
        assertThat(count("SELECT cycles_today FROM data2flow_action.protection_state WHERE device_id = 15")).isEqualTo(1);
    }

    @Test
    @DisplayName("[ACT-02.04][AT-ACT-16.3][TC-ACT-045] 반영된 v8보다 오래된 v7 보고는 버린다")
    void staleReportDiscarded() {
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(Fixtures.AIRCON, 8,
                Map.of("Thermostat", Map.of("mode", "cool", "targetTemperature", 24)), clock.instant(), true));
        await().atMost(Duration.ofSeconds(5)).until(() -> status(commandId).equals("APPLIED"));
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(Fixtures.AIRCON, 7,
                Map.of("Thermostat", Map.of("mode", "off")), clock.instant(), true));
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(Fixtures.AIRCON, 9,
                Map.of("Switch", Map.of("on", true)), clock.instant(), true));

        await().atMost(Duration.ofSeconds(5)).until(() ->
                count("SELECT reported_version FROM data2flow_action.device_shadows WHERE device_id = 15") == 9);
        Result shadow = get(api(Fixtures.USER, Fixtures.ORG), "/internal/action/devices/15/shadow");
        assertThat(shadow.response().path("reported").path("Thermostat").path("mode").asString()).isEqualTo("cool");
    }

    @Test
    @DisplayName("[ACT-02.02][AT-ACT-16.4][TC-ACT-037] 모르는 commandId의 ack는 무시하고 다른 명령에 영향이 없다")
    void unknownAckIgnored() {
        publish(EventType.DEVICE_COMMAND_ACK, DeviceCommandAck.acked("8f1c2d3e-0000-4000-8000-000000000001", Fixtures.AIRCON,
                clock.instant(), true));
        publish(EventType.DEVICE_COMMAND_ACK, DeviceCommandAck.acked("not-a-uuid", Fixtures.AIRCON, clock.instant(), true));
        publish(EventType.DEVICE_COMMAND_ACK, DeviceCommandAck.failed(commandId, Fixtures.AIRCON, "INVALID_COMMAND", clock.instant(), true));

        await().atMost(Duration.ofSeconds(5)).until(() -> status(commandId).equals("FAILED"));
        assertThat(get(api(Fixtures.USER, Fixtures.ORG), "/internal/action/commands/" + commandId).response().path("statusReason")
                .asString()).isEqualTo("INVALID_COMMAND");
        // 이미 끝난 명령에 다시 온 ack는 상태를 바꾸지 않는다
        publish(EventType.DEVICE_COMMAND_ACK, DeviceCommandAck.acked(commandId, Fixtures.AIRCON, clock.instant(), true));
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> status(commandId).equals("FAILED"));
    }
}
