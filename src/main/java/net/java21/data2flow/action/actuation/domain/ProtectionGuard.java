package net.java21.data2flow.action.actuation.domain;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Optional;

/**
 * 장비 보호(ACT-06.01, BR-ACT-10): 최소 꺼짐·켜짐 시간에 걸리면 거부하지 않고 허용 시각까지 미루고(DELAYED), 하루 최대 켜기 횟수를
 * 넘으면 막는다(BLOCKED(PROTECTION)). 전원이 바뀌지 않는 명령은 보지 않는다.
 */
public final class ProtectionGuard {

    private ProtectionGuard() {
    }

    /** 판정 결과. {@code executeAfter}는 DELAY일 때만 */
    public record Decision(Kind kind, Instant executeAfter) {
        public static final Decision PASS = new Decision(Kind.PASS, null);
        public static final Decision BLOCK = new Decision(Kind.BLOCK, null);

        public static Decision delayUntil(Instant at) {
            return new Decision(Kind.DELAY, at);
        }
    }

    public enum Kind { PASS, DELAY, BLOCK }

    /**
     * @param protection 모델 보호 설정. 없으면 PASS
     * @param state      기기 보호 상태
     * @param current    지금 전원(reported 기준). 모르면 빈 값
     * @param target     명령의 전원 목표. 전원과 무관하면 빈 값
     */
    public static Decision evaluate(Protection protection, ProtectionState state, Optional<Boolean> current,
                                    Optional<Boolean> target, Instant now, LocalDate today) {
        if (protection == null || target.isEmpty()) {
            return Decision.PASS;
        }
        ProtectionState s = state == null ? ProtectionState.EMPTY : state;
        boolean turningOn = target.get() && !current.orElse(false);
        boolean turningOff = !target.get() && current.orElse(true);
        if (turningOn) {
            if (protection.maxCyclesPerDay() != null && s.cyclesOn(today) >= protection.maxCyclesPerDay()) {
                return Decision.BLOCK;
            }
            if (protection.minOffSec() != null && s.lastOffAt() != null) {
                Instant allowed = s.lastOffAt().plusSeconds(protection.minOffSec());
                if (now.isBefore(allowed)) {
                    return Decision.delayUntil(allowed);
                }
            }
        } else if (turningOff && protection.minOnSec() != null && s.lastOnAt() != null) {
            Instant allowed = s.lastOnAt().plusSeconds(protection.minOnSec());
            if (now.isBefore(allowed)) {
                return Decision.delayUntil(allowed);
            }
        }
        return Decision.PASS;
    }
}
