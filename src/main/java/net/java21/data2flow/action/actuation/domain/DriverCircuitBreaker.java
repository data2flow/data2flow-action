package net.java21.data2flow.action.actuation.domain;

import java.time.Instant;

/**
 * 드라이버 서킷 브레이커 판정(BR-ACT-14, ACT-07.03). 상태는 {@code driver_circuits}에 두어 파드가 함께 본다. 판정은 순수 함수다.
 *
 * <pre>
 * CLOSED ──(창 안 호출 ≥ minCalls, 실패율 > 기준)──▶ OPEN ──(openSec 지남, 다음 호출)──▶ HALF_OPEN(시험 호출 1건)
 * HALF_OPEN ──성공──▶ CLOSED · ──실패──▶ OPEN(다시 openSec)
 * </pre>
 */
public final class DriverCircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    /**
     * 현재 상태.
     *
     * @param state          상태
     * @param openedAt       열린 시각(OPEN·HALF_OPEN)
     * @param trialStartedAt 시험 호출 시작 시각(HALF_OPEN). 시험이 응답 없이 끝나면 openSec 뒤 다시 시험한다
     */
    public record Circuit(State state, Instant openedAt, Instant trialStartedAt) {
        public static final Circuit CLOSED = new Circuit(State.CLOSED, null, null);
    }

    /** 호출 전 판정 */
    public enum Admission {
        /** 보낸다 */
        ALLOW,
        /** 열림: 즉시 FAILED(DRIVER_UNAVAILABLE) */
        REJECT,
        /** 열림 시간이 지나 시험 호출로 보낸다(상태를 HALF_OPEN으로) */
        TRIAL
    }

    private DriverCircuitBreaker() {
    }

    public static Admission admit(Circuit c, CircuitPolicy policy, Instant now) {
        return switch (c.state()) {
            case CLOSED -> Admission.ALLOW;
            case OPEN -> c.openedAt() == null || !now.isBefore(c.openedAt().plus(policy.open())) ? Admission.TRIAL : Admission.REJECT;
            case HALF_OPEN -> c.trialStartedAt() != null && !now.isBefore(c.trialStartedAt().plus(policy.open()))
                    ? Admission.TRIAL : Admission.REJECT;
        };
    }

    /**
     * 호출 결과 뒤 다음 상태.
     *
     * @param calls    창 안 호출 수(이번 포함)
     * @param failures 창 안 실패 수(이번 포함)
     * @param ok       이번 호출 성공
     */
    public static Circuit after(Circuit c, CircuitPolicy policy, boolean ok, int calls, int failures, Instant now) {
        if (c.state() == State.HALF_OPEN) {
            return ok ? Circuit.CLOSED : new Circuit(State.OPEN, now, null);
        }
        if (c.state() == State.OPEN) {
            return c;   // 열린 동안 끝난 이전 호출의 결과
        }
        if (!ok && calls >= policy.minCalls() && failureRate(calls, failures) * 100 > policy.failureRate()) {
            return new Circuit(State.OPEN, now, null);
        }
        return c;
    }

    public static double failureRate(int calls, int failures) {
        return calls <= 0 ? 0 : (double) failures / calls;
    }
}
