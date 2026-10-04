package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.action.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;

import static net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker.Admission;
import static net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker.Circuit;
import static net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker.State;
import static org.assertj.core.api.Assertions.assertThat;

/** 드라이버 서킷 브레이커(ACT-07.03, BR-ACT-14) */
class DriverCircuitBreakerTest {

    private static final CircuitPolicy POLICY = CircuitPolicy.DEFAULT;   // 1분 실패율 50%, 30초, 최소 5건
    private static final Instant T0 = MutableClock.T0;

    @ParameterizedTest(name = "호출 {0}건·실패 {1}건·이번 성공={2} → {3}")
    @CsvSource({
            "5, 3, false, OPEN",     // 60% > 50% → 열림
            "6, 3, false, CLOSED",   // 50%는 기준 초과가 아님
            "4, 4, false, CLOSED",   // 최소 호출 수(5) 미만
            "10, 9, true, CLOSED",   // 성공한 호출로는 열지 않는다
            "5, 5, false, OPEN"
    })
    @DisplayName("[ACT-07.03][TC-ACT-130] BR-ACT-14 규칙 표: 1분 실패율 50% 초과(최소 5건)면 OPEN")
    void opensOnFailureRate(int calls, int failures, boolean ok, State expected) {
        assertThat(DriverCircuitBreaker.after(Circuit.CLOSED, POLICY, ok, calls, failures, T0).state()).isEqualTo(expected);
    }

    @Test
    @DisplayName("[ACT-07.03][AT-ACT-07.4][TC-ACT-131] OPEN 30초 동안 즉시 거부 → 30초 뒤 HALF_OPEN 시험 호출 → 성공이면 CLOSED, 실패면 다시 OPEN")
    void openHalfOpenClosed() {
        MutableClock clock = new MutableClock(T0);
        Circuit open = DriverCircuitBreaker.after(Circuit.CLOSED, POLICY, false, 6, 6, clock.instant());
        assertThat(open.state()).isEqualTo(State.OPEN);
        assertThat(DriverCircuitBreaker.admit(open, POLICY, clock.instant())).isEqualTo(Admission.REJECT);
        clock.advanceBy(Duration.ofSeconds(29));
        assertThat(DriverCircuitBreaker.admit(open, POLICY, clock.instant())).isEqualTo(Admission.REJECT);
        clock.advanceBy(Duration.ofSeconds(1));
        assertThat(DriverCircuitBreaker.admit(open, POLICY, clock.instant())).isEqualTo(Admission.TRIAL);

        Circuit trial = new Circuit(State.HALF_OPEN, open.openedAt(), clock.instant());
        // 시험 호출이 진행 중이면 다른 호출은 거부
        assertThat(DriverCircuitBreaker.admit(trial, POLICY, clock.instant().plusSeconds(1))).isEqualTo(Admission.REJECT);
        assertThat(DriverCircuitBreaker.after(trial, POLICY, true, 7, 6, clock.instant()).state()).isEqualTo(State.CLOSED);
        Circuit reopened = DriverCircuitBreaker.after(trial, POLICY, false, 7, 7, clock.instant());
        assertThat(reopened.state()).isEqualTo(State.OPEN);
        assertThat(reopened.openedAt()).isEqualTo(clock.instant());
        // 응답 없이 끝난 시험은 30초 뒤 다시 시험한다
        assertThat(DriverCircuitBreaker.admit(trial, POLICY, clock.instant().plusSeconds(30))).isEqualTo(Admission.TRIAL);
        // 열린 동안 끝난 이전 호출 결과는 상태를 바꾸지 않는다
        assertThat(DriverCircuitBreaker.after(open, POLICY, true, 7, 6, clock.instant())).isEqualTo(open);
        assertThat(DriverCircuitBreaker.admit(Circuit.CLOSED, POLICY, clock.instant())).isEqualTo(Admission.ALLOW);
        assertThat(DriverCircuitBreaker.failureRate(0, 0)).isZero();
    }

    @Test
    @DisplayName("[ACT-07.03] 서킷 설정 기본값과 잘못된 값 보정")
    void policyDefaults() {
        CircuitPolicy p = new CircuitPolicy(0, 0, -1, 0);
        assertThat(p).isEqualTo(CircuitPolicy.DEFAULT);
        assertThat(p.window()).isEqualTo(Duration.ofSeconds(60));
        assertThat(p.open()).isEqualTo(Duration.ofSeconds(30));
    }
}
