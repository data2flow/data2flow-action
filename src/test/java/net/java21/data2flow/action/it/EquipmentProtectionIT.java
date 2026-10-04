package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 장비 보호·충돌 방지(ACT-06.01, ACT-02.05)와 사용자 동작(취소·수동 우선 해제·연결 확인). TC-ACT-097·053·054
 */
class EquipmentProtectionIT extends IntegrationTestSupport {

    private RestClient operator;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        operator = api(Fixtures.USER, Fixtures.ORG);
    }

    private Result command(Map<String, Object> args, String key, Map<String, Object> source) {
        Map<String, Object> body = new LinkedHashMap<>(Map.of("deviceId", "15", "capability", "Thermostat", "command", "set", "args", args));
        if (source != null) {
            body.put("source", source);
        }
        return post(operator, "/internal/action/commands", body, key);
    }

    private void reportOff(long version) {
        SIM.versions.put(Fixtures.AIRCON, version);   // 가짜 장비의 다음 보고 버전이 이어지게
        SIM.setState(Fixtures.AIRCON, Map.of("Thermostat", Map.of("mode", "off", "targetTemperature", 24)));
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(Fixtures.AIRCON, version,
                Map.of("Thermostat", Map.of("mode", "off", "targetTemperature", 24)), clock.instant(), true));
        await().atMost(Duration.ofSeconds(20)).until(() ->
                count("SELECT coalesce(max(reported_version), 0) FROM data2flow_action.device_shadows WHERE device_id = 15") == version);
    }

    @Test
    @DisplayName("[ACT-06.01][AT-ACT-08.1][TC-ACT-097] 1분 전에 끈 에어컨 켜기 → DELAYED(PROTECTION, 2분 뒤), 2분 뒤 APPLIED")
    void delayedThenApplied() {
        reportOff(5);
        clock.advanceBy(Duration.ofMinutes(1));

        Result r = command(Map.of("mode", "cool"), "p-1", null);
        String id = r.response().path("id").asString();
        assertThat(r.status()).isEqualTo(202);
        assertThat(r.response().path("status").asString()).isEqualTo("DELAYED");
        assertThat(r.response().path("statusReason").asString()).isEqualTo("PROTECTION");
        assertThat(r.response().path("executeAfter").asString()).isEqualTo(clock.instant().plus(Duration.ofMinutes(2)).toString());
        assertThat(get(operator, "/internal/action/devices/15/control").response().path("pending").get(0).path("status").asString())
                .isEqualTo("DELAYED");

        clock.advanceBy(Duration.ofSeconds(119));
        tracker.processDue();
        assertThat(status(id)).isEqualTo("DELAYED");
        clock.advanceBy(Duration.ofSeconds(1));
        tracker.processDue();

        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("APPLIED"));
    }

    @Test
    @DisplayName("[ACT-02.02] 대기 중(DELAYED) 명령은 취소할 수 있다 → CANCELLED")
    void cancelDelayed() {
        reportOff(5);
        Result r = command(Map.of("mode", "cool"), "p-2", null);
        String id = r.response().path("id").asString();

        Result cancel = post(operator, "/internal/action/commands/" + id + "/cancel", Map.of(), null);

        assertThat(cancel.status()).isEqualTo(200);
        assertThat(cancel.response().path("status").asString()).isEqualTo("CANCELLED");
        clock.advanceBy(Duration.ofMinutes(5));
        tracker.processDue();
        assertThat(SIM.received).isEmpty();
    }

    @Test
    @DisplayName("[ACT-02.05][AT-ACT-02.1][TC-ACT-053] 수동 APPLIED 뒤 플로우(AUTO) 명령은 SKIPPED(MANUAL_OVERRIDE), [자동으로 되돌리기] 뒤 허용")
    void manualOverride() {
        Result manual = command(Map.of("mode", "cool", "targetTemperature", 24), "mo-1", null);
        await().atMost(Duration.ofSeconds(20)).until(() -> status(manual.response().path("id").asString()).equals("APPLIED"));
        Map<String, Object> flow = Map.of("type", "FLOW", "flowId", "f-1", "flowVersion", 3, "nodeId", "n-1");

        Result auto = command(Map.of("mode", "cool", "targetTemperature", 26), "mo-2", flow);
        assertThat(auto.status()).isEqualTo(202);
        assertThat(auto.response().path("status").asString()).isEqualTo("SKIPPED");
        assertThat(auto.response().path("statusReason").asString()).isEqualTo("MANUAL_OVERRIDE");
        assertThat(auto.response().path("priority").asString()).isEqualTo("AUTO");

        Result release = exchange(operator.delete().uri("/internal/action/devices/15/manual-override?capability=Thermostat"));
        assertThat(release.status()).isEqualTo(204);
        clock.advanceBy(Duration.ofSeconds(11));
        Result again = command(Map.of("mode", "cool", "targetTemperature", 26), "mo-3", flow);
        await().atMost(Duration.ofSeconds(20)).until(() -> status(again.response().path("id").asString()).equals("APPLIED"));
    }

    @Test
    @DisplayName("[ACT-02.05][TC-ACT-054] 같은 기기·기능 AUTO 명령을 10초 안에 다시 → 429 COMMAND_RATE_LIMITED + Retry-After, MANUAL은 예외")
    void minInterval() {
        Map<String, Object> flow = Map.of("type", "FLOW", "flowId", "f-1", "flowVersion", 3, "nodeId", "n-1");
        Result first = command(Map.of("mode", "cool", "targetTemperature", 24), "ri-1", flow);
        await().atMost(Duration.ofSeconds(20)).until(() -> status(first.response().path("id").asString()).equals("APPLIED"));
        clock.advanceBy(Duration.ofSeconds(5));

        Result second = command(Map.of("mode", "cool", "targetTemperature", 25), "ri-2", flow);
        Result manual = command(Map.of("mode", "cool", "targetTemperature", 25), "ri-3", null);

        assertThat(second.status()).isEqualTo(429);
        assertThat(second.code()).isEqualTo("COMMAND_RATE_LIMITED");
        assertThat(second.retryAfter()).isEqualTo("5");
        assertThat(second.body().path("header").path("resultMessage").asString()).isEqualTo("5초 후에 다시 시도하세요");
        assertThat(second.response().path("commandId").asString()).isNotBlank();
        assertThat(manual.status()).isEqualTo(202);
    }

    @Test
    @DisplayName("[ACT-03.01][API-ACT-31] virtual 드라이버 연결 확인 → ok·지원 기능, 모르는 종류 404 DRIVER_NOT_FOUND")
    void healthcheck() {
        Result ok = post(operator, "/internal/action/drivers/9/healthcheck", Map.of("type", "VIRTUAL", "config", Map.of()), null);
        Result unknown = post(operator, "/internal/action/drivers/9/healthcheck", Map.of("type", "LG_THINQ", "config", Map.of()), null);

        assertThat(ok.status()).isEqualTo(200);
        assertThat(ok.response().path("ok").asBoolean()).isTrue();
        assertThat(ok.response().path("capabilities").toString()).contains("Thermostat");
        assertThat(unknown.status()).isEqualTo(404);
        assertThat(unknown.code()).isEqualTo("DRIVER_NOT_FOUND");
    }

    @Test
    @DisplayName("[ACT-03.02][BR-ACT-14] 시뮬레이터 일시 장애(503) → 지수 백오프로 최대 3회 호출 뒤 FAILED(DRIVER_ERROR)")
    void retriesThenFails() {
        SIM.modes.put(Fixtures.AIRCON, net.java21.data2flow.action.support.FakeSimulator.Mode.ERROR);
        Result r = command(Map.of("mode", "cool", "targetTemperature", 24), "rt-1", null);
        String id = r.response().path("id").asString();
        assertThat(status(id)).isEqualTo("REQUESTED");

        clock.advanceBy(Duration.ofSeconds(1));
        tracker.processDue();
        clock.advanceBy(Duration.ofSeconds(2));
        tracker.processDue();

        assertThat(status(id)).isEqualTo("FAILED");
        assertThat(SIM.commandsFor(Fixtures.AIRCON)).isEqualTo(3);
    }
}
