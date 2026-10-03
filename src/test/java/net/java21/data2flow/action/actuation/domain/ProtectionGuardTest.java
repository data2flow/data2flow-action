package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.action.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** 장비 보호(ACT-06.01, BR-ACT-10). TC-ACT-098·099 */
class ProtectionGuardTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final LocalDate today = LocalDate.ofInstant(clock.instant(), ZoneOffset.UTC);

    private ProtectionGuard.Decision turnOn(ProtectionState s) {
        return ProtectionGuard.evaluate(Protection.COMPRESSOR, s, Optional.of(false), Optional.of(true), clock.instant(), today);
    }

    @Test
    @DisplayName("[ACT-06.01][AT-ACT-08.1][TC-ACT-099] 끈 지 1분 뒤 켜기 → DELAYED(+2분), advanceBy(2m) 뒤 통과")
    void minOff() {
        Instant offAt = clock.instant();
        clock.advanceBy(Duration.ofMinutes(1));
        ProtectionState s = new ProtectionState(null, offAt, 1, today);

        ProtectionGuard.Decision d = turnOn(s);
        assertThat(d.kind()).isEqualTo(ProtectionGuard.Kind.DELAY);
        assertThat(d.executeAfter()).isEqualTo(offAt.plus(Duration.ofMinutes(3)));

        clock.advanceBy(Duration.ofMinutes(2));
        assertThat(turnOn(s).kind()).isEqualTo(ProtectionGuard.Kind.PASS);
    }

    @Test
    @DisplayName("[ACT-06.01][TC-ACT-099] 켠 지 5분이 안 됐으면 끄기 DELAYED, 하루 반복 한도를 넘으면 BLOCKED(PROTECTION)")
    void minOnAndDailyCycles() {
        ProtectionState on = new ProtectionState(clock.instant(), null, 1, today);
        clock.advanceBy(Duration.ofMinutes(4));
        ProtectionGuard.Decision off = ProtectionGuard.evaluate(Protection.COMPRESSOR, on, Optional.of(true), Optional.of(false),
                clock.instant(), today);
        assertThat(off.kind()).isEqualTo(ProtectionGuard.Kind.DELAY);
        assertThat(off.executeAfter()).isEqualTo(on.lastOnAt().plus(Duration.ofMinutes(5)));

        ProtectionState many = new ProtectionState(null, null, 20, today);
        assertThat(turnOn(many).kind()).isEqualTo(ProtectionGuard.Kind.BLOCK);
        // 날짜가 바뀌면 다시 센다
        assertThat(ProtectionGuard.evaluate(Protection.COMPRESSOR, many, Optional.of(false), Optional.of(true), clock.instant(),
                today.plusDays(1)).kind()).isEqualTo(ProtectionGuard.Kind.PASS);
    }

    @Test
    @DisplayName("[ACT-06.01][BR-ACT-10][TC-ACT-098] 보호 설정이 없거나 전원이 바뀌지 않는 명령(목표 온도만)은 보지 않는다")
    void notApplicable() {
        ProtectionState s = new ProtectionState(null, clock.instant(), 30, today);
        assertThat(ProtectionGuard.evaluate(null, s, Optional.of(false), Optional.of(true), clock.instant(), today).kind())
                .isEqualTo(ProtectionGuard.Kind.PASS);
        assertThat(ProtectionGuard.evaluate(Protection.COMPRESSOR, s, Optional.of(true), Optional.empty(), clock.instant(), today).kind())
                .isEqualTo(ProtectionGuard.Kind.PASS);
        assertThat(ProtectionGuard.evaluate(Protection.COMPRESSOR, s, Optional.of(true), Optional.of(true), clock.instant(), today).kind())
                .isEqualTo(ProtectionGuard.Kind.PASS);   // 이미 켜짐 → 켜기는 반복이 아님
        assertThat(ProtectionGuard.evaluate(Protection.COMPRESSOR, null, Optional.empty(), Optional.of(true), clock.instant(), today).kind())
                .isEqualTo(ProtectionGuard.Kind.PASS);
    }

    @Test
    @DisplayName("[ACT-06.01] 보고에 따른 보호 상태 갱신: 켜짐은 횟수를 더하고, 꺼짐은 꺼진 시각만")
    void withPower() {
        ProtectionState s = ProtectionState.EMPTY.withPower(true, clock.instant(), today).withPower(false, clock.instant(), today)
                .withPower(true, clock.instant(), today);
        assertThat(s.cyclesOn(today)).isEqualTo(2);
        assertThat(s.lastOffAt()).isEqualTo(clock.instant());
        assertThat(s.cyclesOn(today.plusDays(1))).isZero();
    }
}
