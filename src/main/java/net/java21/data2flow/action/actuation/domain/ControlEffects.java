package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.capability.CapabilityDefinition;
import net.java21.data2flow.contracts.capability.ExpectedEffect;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 제어 효과 확인(BR-ACT-20, ACT-08.01). 기능 정의의 {@code expectedEffects} 중 명령과 인자가 맞는 것을 고르고, APPLIED 뒤 정해진 시간 안의
 * 측정값 변화가 기대 방향으로 {@code minChange} 이상인지 본다.
 */
public final class ControlEffects {

    /** 판정 */
    public enum Verdict { EFFECTIVE, NO_EFFECT, UNKNOWN }

    private ControlEffects() {
    }

    /** 이 명령에 기대하는 효과. 없으면 빈 값 */
    public static Optional<ExpectedEffect> expected(CapabilityCatalog catalog, String capability, String command, Map<String, ?> args) {
        Optional<CapabilityDefinition> def = catalog.find(capability);
        if (def.isEmpty()) {
            return Optional.empty();
        }
        for (ExpectedEffect e : def.get().expectedEffects()) {
            if (e.when() == null || !Objects.equals(e.when().command(), command)) {
                continue;
            }
            boolean all = e.when().args().entrySet().stream()
                    .allMatch(w -> args != null && args.get(w.getKey()) != null && w.getValue().toString().equals(args.get(w.getKey()).toString()));
            if (all) {
                return Optional.of(e);
            }
        }
        return Optional.empty();
    }

    /**
     * @param start     APPLIED 때 값
     * @param end       확인 시각 값
     * @param minChange 기대 방향 최소 변화(절댓값)
     */
    public static Verdict judge(ExpectedEffect.Direction direction, Double start, Double end, double minChange) {
        if (start == null || end == null) {
            return Verdict.UNKNOWN;
        }
        double delta = end - start;
        double eps = 1e-9;   // 부동소수 경계(-0.2 = 26.8 - 27.0)
        boolean ok = direction == ExpectedEffect.Direction.DOWN ? delta <= -minChange + eps : delta >= minChange - eps;
        return ok ? Verdict.EFFECTIVE : Verdict.NO_EFFECT;
    }
}
