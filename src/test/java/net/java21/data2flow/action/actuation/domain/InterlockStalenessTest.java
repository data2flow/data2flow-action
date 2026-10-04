package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.action.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** 인터락 "조건 데이터 없음" 판정 시간(BR-ACT-11) */
class InterlockStalenessTest {

    private static final Instant NOW = MutableClock.T0;

    private static boolean blocked(Interlock il, InterlockEvaluator.Input in) {
        return InterlockEvaluator.evaluate(List.of(il), "Thermostat", "set", Map.of("mode", "cool"), c -> List.of(Optional.of(in)), NOW)
                .blocked();
    }

    /** 실내 온도 15℃ 미만이면 냉방 금지(조건 거짓인 값으로 데이터 나이만 본다) */
    private static Interlock cold(Integer staleAfterSec) {
        return new Interlock(7L, "저온 냉방 금지", 31L, true, new Interlock.Condition("metric", 40L, null, null, "temperature", null, null,
                "<", 15), new Interlock.Forbid("Thermostat", null, null), "실내가 추워 냉방을 켤 수 없습니다", staleAfterSec);
    }

    @Test
    @DisplayName("[ACT-06.02][AT-ACT-08.4][TC-ACT-137] 10분 주기 센서: 25분 경과 → 마지막 값 사용, 31분 → BLOCKED(조건 데이터 없음)")
    void reportIntervalTimesThree() {
        assertThat(blocked(cold(null), new InterlockEvaluator.Input(22.0, NOW.minus(Duration.ofMinutes(25)), 600, false, false))).isFalse();
        assertThat(blocked(cold(null), new InterlockEvaluator.Input(22.0, NOW.minus(Duration.ofMinutes(31)), 600, false, false))).isTrue();
        // 1분 주기면 최소 5분이 기준
        assertThat(InterlockEvaluator.staleAfter(cold(null), 60)).isEqualTo(Duration.ofMinutes(5));
        // 규칙이 더 길게 정하면 그 값(짧게는 못 줄임)
        assertThat(InterlockEvaluator.staleAfter(cold(3600), 600)).isEqualTo(Duration.ofHours(1));
        assertThat(InterlockEvaluator.staleAfter(cold(60), 600)).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    @DisplayName("[ACT-06.02][AT-ACT-08.5][TC-ACT-137] 이벤트형 Contact: ONLINE이면 오래된 마지막 상태를 쓰고, OFFLINE이면 BLOCKED")
    void eventDrivenContact() {
        Interlock il = InterlockEvaluatorTest.windowOpen();
        var closedOnline = new InterlockEvaluator.Input(false, NOW.minus(Duration.ofDays(2)), 600, true, false);
        assertThat(InterlockEvaluator.evaluate(List.of(il), "Thermostat", "set", Map.of("mode", "cool"), c -> List.of(Optional.of(closedOnline)),
                NOW).blocked()).isFalse();
        var closedOffline = new InterlockEvaluator.Input(false, NOW.minus(Duration.ofMinutes(1)), 600, true, true);
        var d = InterlockEvaluator.evaluate(List.of(il), "Thermostat", "set", Map.of("mode", "cool"), c -> List.of(Optional.of(closedOffline)), NOW);
        assertThat(d.blocked()).isTrue();
        assertThat(d.reason()).isEqualTo(InterlockEvaluator.Reason.STALE_DATA);
        // 값이 null(보고 없음)이면 안전 쪽
        assertThat(blocked(cold(null), new InterlockEvaluator.Input(null, NOW, 600, false, false))).isTrue();
    }
}
