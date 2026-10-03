package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.command.CommandPriority;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 충돌·폭주 방지(ACT-02.05, BR-ACT-07·08). TC-ACT-049·050·051·052·053 */
class RateGuardsTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");

    @Test
    @DisplayName("[ACT-02.05][TC-ACT-051] 같은 기기·기능 AUTO 명령: 9.9초 뒤 막힘(남은 시간), 10.0초 뒤 허용, MANUAL·SAFETY는 간격 무시")
    void minInterval() {
        Instant last = clock.instant();
        clock.advanceBy(Duration.ofMillis(9_900));
        assertThat(RateGuards.minInterval(CommandPriority.AUTO, last, 10, clock.instant())).contains(Duration.ofMillis(100));
        clock.advanceBy(Duration.ofMillis(100));
        assertThat(RateGuards.minInterval(CommandPriority.AUTO, last, 10, clock.instant())).isEmpty();
        clock.set(last.plusSeconds(1));
        assertThat(RateGuards.minInterval(CommandPriority.MANUAL, last, 10, clock.instant())).isEmpty();
        assertThat(RateGuards.minInterval(CommandPriority.SAFETY, last, 10, clock.instant())).isEmpty();
        assertThat(RateGuards.minInterval(CommandPriority.SCHEDULE, last, 10, clock.instant())).isPresent();
        assertThat(RateGuards.minInterval(CommandPriority.AI, last, 10, clock.instant())).isPresent();
        assertThat(RateGuards.minInterval(CommandPriority.AUTO, null, 10, clock.instant())).isEmpty();
        assertThat(RateGuards.minInterval(CommandPriority.AUTO, last, 0, clock.instant())).isEmpty();
    }

    @Test
    @DisplayName("[ACT-02.05][AT-ACT-02.4][TC-ACT-052] 60초 안 on→off→on 세 번째 BLOCKED(OSCILLATION), 같은 방향 반복은 진동 아님")
    void oscillation() {
        assertThat(RateGuards.oscillating(CommandPriority.AUTO, List.of(true, false), true, 3)).isTrue();
        assertThat(RateGuards.oscillating(CommandPriority.AUTO, List.of(false), true, 3)).isFalse();
        assertThat(RateGuards.oscillating(CommandPriority.AUTO, List.of(true, true), true, 3)).isFalse();
        assertThat(RateGuards.oscillating(CommandPriority.AUTO, List.of(true, false, false), true, 3)).isFalse();
        assertThat(RateGuards.oscillating(CommandPriority.MANUAL, List.of(true, false), true, 3)).isTrue();
        assertThat(RateGuards.oscillating(CommandPriority.SAFETY, List.of(true, false), true, 3)).isFalse();
    }

    @ParameterizedTest(name = "{0} 수동 우선 중 → 막힘 {1} (예약 존중 {2})")
    @CsvSource({"AUTO,true,true", "AI,true,true", "SCHEDULE,true,true", "SCHEDULE,false,false", "MANUAL,false,true", "SAFETY,false,true"})
    @DisplayName("[ACT-02.05][BR-ACT-08][TC-ACT-050] 수동 우선 중 AUTO·AI는 SKIPPED, SCHEDULE은 조직 설정, MANUAL·SAFETY는 허용")
    void manualOverride(CommandPriority priority, boolean blocked, boolean scheduleRespects) {
        Instant until = clock.instant().plus(Duration.ofMinutes(30));
        assertThat(RateGuards.manualOverrideBlocks(priority, until, clock.instant(), scheduleRespects)).isEqualTo(blocked);
    }

    @Test
    @DisplayName("[ACT-02.05][AT-ACT-10.2][TC-ACT-053] advanceBy(30m) 뒤 수동 우선이 저절로 풀린다")
    void manualOverrideExpires() {
        Instant until = clock.instant().plus(Duration.ofMinutes(30));
        clock.advanceBy(Duration.ofMinutes(30));
        assertThat(RateGuards.manualOverrideBlocks(CommandPriority.AUTO, until, clock.instant(), true)).isFalse();
        assertThat(RateGuards.manualOverrideBlocks(CommandPriority.AUTO, null, clock.instant(), true)).isFalse();
    }
}
