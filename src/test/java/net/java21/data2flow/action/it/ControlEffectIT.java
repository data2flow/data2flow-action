package net.java21.data2flow.action.it;

import net.java21.data2flow.action.actuation.service.ControlEffectService;
import net.java21.data2flow.action.actuation.service.RuntimeStatsService;
import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestClient;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 제어 효과 확인(ACT-08.01, TC-ACT-132)과 가동 집계(ACT-08.02) */
class ControlEffectIT extends IntegrationTestSupport {

    @Autowired
    ControlEffectService effects;
    @Autowired
    RuntimeStatsService runtime;

    private RestClient operator;
    private volatile Instant appliedAt;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        operator = api(Fixtures.USER, Fixtures.ORG);
        // 실습실 온도: 냉방 적용 시각 27.0℃, 15분 뒤 27.1℃(효과 없음)
        CORE.routes.put("/internal/core/metric-values", req -> {
            String query = URLDecoder.decode(req.getRequestUrl().query(), StandardCharsets.UTF_8);
            String at = query.replaceAll(".*at=([^&]*).*", "$1");
            double value = appliedAt != null && Instant.parse(at).isAfter(appliedAt.plusSeconds(60)) ? 27.1 : 27.0;
            return FakeCore.ok(Map.of("metric", "temperature", "value", value, "measuredAt", at));
        });
    }

    private String coolNow(String key) {
        Result r = post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", Map.of("mode", "cool", "targetTemperature", 24)), key);
        String id = r.response().path("id").asString();
        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("APPLIED"));
        return id;
    }

    @Test
    @DisplayName("[ACT-08.01][AT-ACT-12.1][TC-ACT-132] 냉방 APPLIED, 15분간 +0.1℃ → NO_EFFECT 이벤트 1건, 기기 이력(runtime)에 표시, 1시간 안 두 번째는 이벤트 없음")
    void noEffect() {
        listen("command.no-effect");
        appliedAt = clock.instant();
        String id = coolNow("e-1");
        assertThat(count("SELECT count(*) FROM data2flow_action.effect_checks WHERE status = 'PENDING'")).isEqualTo(1);

        clock.advanceBy(Duration.ofMinutes(14));
        assertThat(effects.processDue()).isZero();
        clock.advanceBy(Duration.ofMinutes(1));
        assertThat(effects.processDue()).isEqualTo(1);

        await().atMost(Duration.ofSeconds(20)).until(() -> events("command.no-effect").size() == 1);
        var payload = events("command.no-effect").get(0).path("payload");
        assertThat(payload.path("commandId").asString()).isEqualTo(id);
        assertThat(payload.path("expected").path("metric").asString()).isEqualTo("temperature");
        assertThat(payload.path("observed").path("delta").asDouble()).isCloseTo(0.1, org.assertj.core.data.Offset.offset(1e-9));

        Result rt = get(operator, "/internal/action/devices/15/runtime?from=2026-03-01&to=2026-03-03");
        assertThat(rt.response().path("noEffectEvents")).hasSize(1);

        // 같은 기기 1시간 안 두 번째 효과 없음: 기록은 남고 이벤트는 내지 않는다(BR-ACT-20)
        clock.advanceBy(Duration.ofMinutes(5));
        post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", Map.of("mode", "fan")), "e-2");
        appliedAt = clock.instant();
        String second = coolNow("e-3");
        clock.advanceBy(Duration.ofMinutes(15));
        effects.processDue();
        assertThat(count("SELECT count(*) FROM data2flow_action.effect_checks WHERE status = 'NO_EFFECT'")).isEqualTo(2);
        assertThat(count("SELECT count(*) FROM data2flow_action.effect_checks WHERE event_emitted")).isEqualTo(1);
        assertThat(second).isNotBlank();
    }

    @Test
    @DisplayName("[ACT-08.02][AT-ACT-12.2] 가동 집계: 상태 구간으로 하루 가동 시간·켜짐 횟수를 계산한다")
    void runtimeAggregation() {
        coolNow("r-1");
        clock.advanceBy(Duration.ofHours(2));
        runtime.aggregateRecent();
        var items = get(operator, "/internal/action/devices/15/runtime?from=2026-03-01&to=2026-03-03").response().path("items");
        assertThat(items).isNotEmpty();
        long onSeconds = 0;
        for (var i : items) {
            onSeconds += i.path("onSeconds").asLong();
        }
        assertThat(onSeconds).isEqualTo(7200);
        assertThat(runtime.aggregate(Fixtures.ORG, Fixtures.AIRCON, LocalDate.of(2026, 3, 2)).cycles()).isEqualTo(1);
    }
}
