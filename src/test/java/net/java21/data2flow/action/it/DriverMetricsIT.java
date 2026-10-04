package net.java21.data2flow.action.it;

import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.service.DriverHealthService;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** 드라이버 상태 모니터링(ACT-03.06, TC-ACT-081) */
class DriverMetricsIT extends IntegrationTestSupport {

    @Autowired
    DriverHealthService health;

    @Test
    @DisplayName("[ACT-03.06][TC-ACT-081] 호출 100건 중 실패 10건·평균 120ms → 1시간 지표 오류율 10%·평균 120ms·p95, 1시간 1분 전 호출은 제외")
    void metrics() {
        DriverBinding binding = new DriverBinding(9L, "VIRTUAL", Map.of(), 30, 60, null);
        health.record(Fixtures.ORG, binding, UUID.randomUUID(), false, 9_000, "오래된 실패");   // 1시간 1분 전
        clock.advanceBy(Duration.ofMinutes(61));
        for (int i = 0; i < 100; i++) {
            boolean ok = i % 10 != 5;
            health.record(Fixtures.ORG, binding, UUID.randomUUID(), ok, i % 2 == 0 ? 100 : 140, ok ? null : "HTTP 503");
        }
        CORE.roles.put(Fixtures.USER, "ADMIN");

        Result r = get(api(Fixtures.USER, Fixtures.ORG), "/internal/action/drivers/9/metrics?window=1h");

        assertThat(r.status()).isEqualTo(200);
        assertThat(r.response().path("requests").asInt()).isEqualTo(100);
        assertThat(r.response().path("errors").asInt()).isEqualTo(10);
        assertThat(r.response().path("errorRate").asDouble()).isEqualTo(0.1);
        assertThat(r.response().path("avgMs").asDouble()).isBetween(115.0, 125.0);
        assertThat(r.response().path("p95Ms").asDouble()).isEqualTo(140.0);
        assertThat(r.response().path("status").asString()).isEqualTo("OK");
        assertThat(r.response().path("circuit").path("state").asString()).isEqualTo("CLOSED");
        assertThat(r.response().path("recentErrors").get(0).path("message").asString()).isEqualTo("HTTP 503");
        // 24시간 창은 오래된 호출도 포함
        assertThat(get(api(Fixtures.USER, Fixtures.ORG), "/internal/action/drivers/9/metrics?window=24h").response().path("requests").asInt())
                .isEqualTo(101);
        // DRIVER_MANAGE가 없으면 403
        CORE.roles.put(901L, "OPERATOR");   // 권한 캐시(10초)가 다른 시험과 섞이지 않게 이 시험만 쓰는 사용자
        assertThat(get(api(901, Fixtures.ORG), "/internal/action/drivers/9/metrics").status()).isEqualTo(403);
        // 호출이 없는 드라이버는 UNTESTED
        assertThat(get(api(Fixtures.USER, Fixtures.ORG), "/internal/action/drivers/77/metrics").response().path("status").asString())
                .isEqualTo("UNTESTED");
        assertThat(health.purge()).isZero();
    }
}
