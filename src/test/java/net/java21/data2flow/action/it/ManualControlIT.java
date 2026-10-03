package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.FakeSimulator;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
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
 * UC-ACT-01 수동 제어(내부 API {@code POST /internal/action/commands}, core-api가 넘김) 주 흐름·예외 흐름.
 * ACT-01.03·02.01·02.02·02.04·04.03·06.04, IAM-06.01
 */
class ManualControlIT extends IntegrationTestSupport {

    private RestClient operator;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        CORE.roles.put(8L, "ANALYST");
        operator = api(Fixtures.USER, Fixtures.ORG);
    }

    private Result cool(RestClient client, Object temperature, String key, String wait) {
        Map<String, Object> body = new java.util.LinkedHashMap<>(Map.of("deviceId", String.valueOf(Fixtures.AIRCON),
                "capability", "Thermostat", "command", "set", "args", Map.of("mode", "cool", "targetTemperature", temperature)));
        if (wait != null) {
            body.put("wait", wait);
        }
        return post(client, "/internal/action/commands", body, key);
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-01.1][TC-ACT-014] 냉방 24℃ 적용 → 5초 안에 APPLIED, reported.mode=cool·targetTemperature=24")
    void appliedWithinFiveSeconds() {
        Result r = cool(operator, 24, "click-0001", "applied");

        assertThat(r.status()).as(String.valueOf(r.body())).isEqualTo(200);
        assertThat(r.response().path("status").asString()).isEqualTo("APPLIED");
        assertThat(r.response().path("priority").asString()).isEqualTo("MANUAL");
        assertThat(r.response().path("source").path("type").asString()).isEqualTo("USER");
        assertThat(r.response().path("source").path("userId").asString()).isEqualTo("7");
        List<String> timeline = r.response().path("timeline").valueStream().map(n -> n.path("status").asString()).toList();
        assertThat(timeline).containsExactly("REQUESTED", "SENT", "ACKED", "APPLIED");
        Result shadow = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/shadow");
        assertThat(shadow.response().path("reported").path("Thermostat").path("mode").asString()).isEqualTo("cool");
        assertThat(shadow.response().path("reported").path("Thermostat").path("targetTemperature").asInt()).isEqualTo(24);
        assertThat(shadow.response().path("delta").isEmpty()).isTrue();
        assertThat(shadow.response().path("desiredSource").path("type").asString()).isEqualTo("USER");
        // EVT-ACT-01 상태별 라우팅 키, EVT-ACT-02 1회
        assertThat(events("command.status.applied")).hasSize(1);
        assertThat(events("device.state.changed")).hasSize(1);
        // 수동 우선 30분(BR-ACT-08)
        Result control = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/control");
        assertThat(control.response().path("manualOverride").path("capability").asString()).isEqualTo("Thermostat");
        assertThat(control.response().path("controllable").asBoolean()).isTrue();
        assertThat(control.response().path("capabilities").get(0).path("effectiveConstraints").path("targetTemperature").path("max")
                .asDouble()).isEqualTo(28.0);
        // 감사(IAM-06.01 DEVICE_COMMAND)는 아웃박스로 core에 간다
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            relay.relayOnce();
            return CORE.audits.stream().anyMatch(a -> a.path("action").asString().equals("DEVICE_COMMAND"));
        });
    }

    @Test
    @DisplayName("[ACT-06.04][AT-ACT-01.3][TC-ACT-113] 조직 절대 한계 18~28에서 29℃ → 400 COMMAND_ABSOLUTE_LIMIT, 이력에 REJECTED")
    void absoluteLimitRejectedAndRecorded() {
        Result r = cool(operator, 29, "click-0002", null);

        assertThat(r.status()).isEqualTo(400);
        assertThat(r.code()).isEqualTo("COMMAND_ABSOLUTE_LIMIT");
        assertThat(r.body().path("header").path("resultMessage").asString()).contains("18").contains("28");
        assertThat(r.response().path("status").asString()).isEqualTo("REJECTED");
        assertThat(r.response().path("statusReason").asString()).isEqualTo("ABSOLUTE_LIMIT");
        Result history = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/commands");
        assertThat(history.body().path("responses").get(0).path("status").asString()).isEqualTo("REJECTED");
        assertThat(SIM.received).isEmpty();
    }

    @Test
    @DisplayName("[ACT-01.03][AT-ACT-01.2][TC-ACT-004] 모델 범위 18~30에서 31℃ → 400 COMMAND_ARG_OUT_OF_RANGE(18~30)")
    void modelConstraint() {
        Result r = cool(operator, 31, "click-0003", null);

        assertThat(r.status()).isEqualTo(400);
        assertThat(r.code()).isEqualTo("COMMAND_ARG_OUT_OF_RANGE");
        assertThat(r.body().path("header").path("resultMessage").asString()).isEqualTo("18~30 사이로 설정하세요");
        assertThat(r.body().path("errors").get(0).path("field").asString()).isEqualTo("args.targetTemperature");
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-01.5][TC-ACT-017] 같은 Idempotency-Key로 2번 → 명령 1건, 두 번째 응답은 첫 명령 ID")
    void idempotencyKey() {
        Result first = cool(operator, 24, "click-0004", null);
        Result second = cool(operator, 24, "click-0004", null);

        assertThat(first.status()).isEqualTo(202);
        assertThat(second.response().path("id").asString()).isEqualTo(first.response().path("id").asString());
        assertThat(count("SELECT count(*) FROM data2flow_action.commands")).isEqualTo(1);
        assertThat(SIM.commandsFor(Fixtures.AIRCON)).isEqualTo(1);
        // 다른 사용자가 같은 키를 써도 섞이지 않는다
        CORE.roles.put(9L, "OPERATOR");
        Result other = cool(api(9L, Fixtures.ORG), 25, "click-0004", null);
        assertThat(other.response().path("id").asString()).isNotEqualTo(first.response().path("id").asString());
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-01.4][TC-ACT-015] ANALYST → 403 PERMISSION_DENIED, 다른 조직 → 404 DEVICE_NOT_FOUND")
    void permission() {
        Result analyst = cool(api(8L, Fixtures.ORG), 24, "click-0005", null);
        Result otherOrg = cool(api(Fixtures.USER, Fixtures.OTHER_ORG), 24, "click-0006", null);

        assertThat(analyst.status()).isEqualTo(403);
        assertThat(analyst.code()).isEqualTo("PERMISSION_DENIED");
        assertThat(otherOrg.status()).isEqualTo(404);
        assertThat(otherOrg.code()).isEqualTo("DEVICE_NOT_FOUND");
        assertThat(count("SELECT count(*) FROM data2flow_action.commands")).isZero();
    }

    @Test
    @DisplayName("[ACT-02.02][AT-ACT-01.6][TC-ACT-034] 응답 실패 확률 100% 가상 장비 → 30초 뒤 TIMEOUT(TIMEOUT_ACK)")
    void timeout() {
        SIM.modes.put(Fixtures.AIRCON, FakeSimulator.Mode.SILENT);
        Result r = cool(operator, 24, "click-0007", null);
        String id = r.response().path("id").asString();
        assertThat(status(id)).isEqualTo("SENT");

        clock.advanceBy(Duration.ofSeconds(29));
        tracker.processDue();
        assertThat(status(id)).isEqualTo("SENT");
        clock.advanceBy(Duration.ofSeconds(2));
        tracker.processDue();

        assertThat(status(id)).isEqualTo("TIMEOUT");
        Result detail = get(operator, "/internal/action/commands/" + id);
        assertThat(detail.response().path("statusReason").asString()).isEqualTo("TIMEOUT_ACK");
        assertThat(events("command.status.timeout")).hasSize(1);
    }

    @Test
    @DisplayName("[ACT-04.03][AT-ACT-14.1] 기기별 명령 이력은 최신순 커서 목록이고 거부된 명령도 남는다")
    void historyCursor() {
        cool(operator, 29, "h-1", null);
        clock.advanceBy(Duration.ofSeconds(1));
        cool(operator, 31, "h-2", null);
        clock.advanceBy(Duration.ofSeconds(1));
        cool(operator, 24, "h-3", null);

        Result page1 = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/commands?size=2");
        assertThat(page1.body().path("responses")).hasSize(2);
        assertThat(page1.body().path("responses").get(0).path("args").path("targetTemperature").asInt()).isEqualTo(24);
        String cursor = page1.body().path("nextCursor").asString();
        Result page2 = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/commands?size=2&cursor=" + cursor);
        assertThat(page2.body().path("responses")).hasSize(1);
        assertThat(page2.body().path("nextCursor").isNull() || page2.body().path("nextCursor").isMissingNode()).isTrue();
        Result rejected = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/commands?status=rejected");
        assertThat(rejected.body().path("responses")).hasSize(2);
        Result bad = get(operator, "/internal/action/devices/" + Fixtures.AIRCON + "/commands?cursor=@@");
        assertThat(bad.status()).isEqualTo(400);
    }

    @Test
    @DisplayName("[ACT-02.02] 없는 명령 404 COMMAND_NOT_FOUND, 끝난 명령 취소 409 COMMAND_NOT_CANCELLABLE")
    void commandErrors() {
        Result missing = get(operator, "/internal/action/commands/00000000-0000-4000-8000-000000000000");
        assertThat(missing.status()).isEqualTo(404);
        assertThat(missing.code()).isEqualTo("COMMAND_NOT_FOUND");
        Result applied = cool(operator, 24, "c-1", "applied");
        Result cancel = post(operator, "/internal/action/commands/" + applied.response().path("id").asString() + "/cancel", Map.of(), null);
        assertThat(cancel.status()).isEqualTo(409);
        assertThat(cancel.code()).isEqualTo("COMMAND_NOT_CANCELLABLE");
        JsonNode body = get(operator, "/internal/action/commands/not-a-uuid").body();
        assertThat(body.path("header").path("resultCode").asString()).isEqualTo("COMMAND_NOT_FOUND");
    }
}
