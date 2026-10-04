package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.FakeSimulator;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceChanged;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 상태 쌍 동기화(ACT-02.04, UC-ACT-03): 오프라인 대기, 재연결 재적용(BR-ACT-06), 기기에서 직접 바뀐 상태. TC-ACT-042·043
 */
class DeviceShadowSyncIT extends IntegrationTestSupport {

    private RestClient operator;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.aircon(Fixtures.AIRCON, Fixtures.ORG, true, true));
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        operator = api(Fixtures.USER, Fixtures.ORG);
    }

    private Result set(Map<String, Object> args, String key) {
        return post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", args), key);
    }

    private void connectivity(DeviceConnectivityChanged.Connectivity to) {
        publish(EventType.DEVICE_CONNECTIVITY_CHANGED, new DeviceConnectivityChanged(Fixtures.AIRCON, null, to, clock.instant(), 300, 3));
        await().atMost(Duration.ofSeconds(20)).until(() -> jdbc.sql("SELECT connectivity FROM data2flow_action.device_shadows WHERE device_id = 15")
                .query(String.class).optional().map(to.name()::equals).orElse(false));
    }

    @Test
    @DisplayName("[ACT-02.04][AT-ACT-03.2][TC-ACT-042] 오프라인 중 desired 변경 → 대기(새 명령이 이전 대기 명령 대체) → 재연결 시 전송 → delta 없음")
    void queuedThenReappliedOnReconnect() {
        connectivity(DeviceConnectivityChanged.Connectivity.OFFLINE);
        Result first = set(Map.of("mode", "cool", "targetTemperature", 25), "off-1");
        clock.advanceBy(Duration.ofSeconds(1));
        Result second = set(Map.of("mode", "cool", "targetTemperature", 24), "off-2");
        assertThat(first.response().path("status").asString()).isEqualTo("QUEUED");
        assertThat(status(first.response().path("id").asString())).isEqualTo("SUPERSEDED");
        assertThat(second.response().path("status").asString()).isEqualTo("QUEUED");
        assertThat(SIM.received).isEmpty();

        connectivity(DeviceConnectivityChanged.Connectivity.ONLINE);

        String id = second.response().path("id").asString();
        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("APPLIED"));
        assertThat(SIM.commandsFor(Fixtures.AIRCON)).isEqualTo(1);
        Result shadow = get(operator, "/internal/action/devices/15/shadow");
        assertThat(shadow.response().path("delta").isEmpty()).isTrue();
        assertThat(shadow.response().path("connectivity").asString()).isEqualTo("ONLINE");
    }

    @Test
    @DisplayName("[ACT-02.04][BR-ACT-06][TC-ACT-048] 재적용 켜진 모델: 응답 없이 끝난 desired가 남아 있으면 재연결 때 delta 명령 1건")
    void reapplyDelta() {
        SIM.modes.put(Fixtures.AIRCON, FakeSimulator.Mode.SILENT);
        Result r = set(Map.of("mode", "cool", "targetTemperature", 24), "re-1");
        clock.advanceBy(Duration.ofSeconds(31));
        tracker.processDue();
        assertThat(status(r.response().path("id").asString())).isEqualTo("TIMEOUT");
        connectivity(DeviceConnectivityChanged.Connectivity.OFFLINE);
        SIM.modes.put(Fixtures.AIRCON, FakeSimulator.Mode.RESPOND);

        connectivity(DeviceConnectivityChanged.Connectivity.ONLINE);

        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.commands WHERE status = 'APPLIED'") == 1);
        JsonNode reapplied = SIM.received.get(SIM.received.size() - 1);
        assertThat(reapplied.path("args").path("targetTemperature").asInt()).isEqualTo(24);
        // 원래 명령의 출처(USER)·우선순위(MANUAL)를 따른다
        assertThat(jdbc.sql("SELECT priority FROM data2flow_action.commands WHERE status = 'APPLIED'").query(String.class).single())
                .isEqualTo("MANUAL");
    }

    @Test
    @DisplayName("[ACT-02.04][AT-ACT-03.3][TC-ACT-043] 리모컨으로 온도 변경(reported만) → delta 표시, origin DEVICE_LOCAL, desired 그대로")
    void deviceLocalChange() {
        Result r = set(Map.of("mode", "cool", "targetTemperature", 24), "loc-1");
        await().atMost(Duration.ofSeconds(20)).until(() -> status(r.response().path("id").asString()).equals("APPLIED"));

        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(Fixtures.AIRCON, 50,
                Map.of("Thermostat", Map.of("mode", "cool", "targetTemperature", 26)), clock.instant(), true));

        await().atMost(Duration.ofSeconds(20)).until(() ->
                count("SELECT coalesce(max(reported_version), 0) FROM data2flow_action.device_shadows WHERE device_id = 15") == 50);
        Result shadow = get(operator, "/internal/action/devices/15/shadow");
        assertThat(shadow.response().path("desired").path("Thermostat").path("targetTemperature").asInt()).isEqualTo(24);
        assertThat(shadow.response().path("delta").path("Thermostat").path("targetTemperature").asInt()).isEqualTo(24);
        List<JsonNode> changed = events("device.state.changed");
        assertThat(changed.get(changed.size() - 1).path("payload").path("origin").asString()).isEqualTo("DEVICE_LOCAL");
        assertThat(SIM.commandsFor(Fixtures.AIRCON)).isEqualTo(1);   // 자동 재적용 안 함
    }

    @Test
    @DisplayName("[DEV-01.01] 기기 삭제(device.changed DELETED) → 상태 쌍·수동 우선·보호 상태 정리, 명령 이력은 남긴다")
    void deviceDeletedCleansUp() {
        Result r = set(Map.of("mode", "cool", "targetTemperature", 24), "del-1");
        await().atMost(Duration.ofSeconds(20)).until(() -> status(r.response().path("id").asString()).equals("APPLIED"));

        publish(EventType.DEVICE_CHANGED, new DeviceChanged(Fixtures.AIRCON, DeviceChanged.Change.DELETED, List.of(), "DELETED", "3",
                Fixtures.SPACE, 9));

        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.device_shadows") == 0);
        assertThat(count("SELECT count(*) FROM data2flow_action.manual_overrides")).isZero();
        assertThat(count("SELECT count(*) FROM data2flow_action.commands")).isEqualTo(1);
    }
}
