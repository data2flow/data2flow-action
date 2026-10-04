package net.java21.data2flow.action.actuation.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * 인터락 판정(BR-ACT-11, ACT-06.02). 금지 대상 명령이고 조건이 참이면 막는다. 조건 판단에 필요한 데이터가 "보고 주기 × 3"(최소 5분, 규칙이
 * 더 길게 정할 수 있음)을 넘게 없으면 안전 쪽으로 막는다. 이벤트형 센서(Contact처럼 바뀔 때만 보고)는 마지막 상태를 쓰고, 기기가
 * OFFLINE일 때만 안전 쪽으로 막는다.
 */
public final class InterlockEvaluator {

    /** 데이터 없음 판정 최소 시간 */
    public static final Duration MIN_STALE = Duration.ofMinutes(5);
    /** 바뀔 때만 보고하는 기능(BR-ACT-11 이벤트형 센서) */
    public static final List<String> EVENT_CAPABILITIES = List.of("Contact");

    /**
     * 조건 입력 하나.
     *
     * @param value             값(측정값 또는 상태 속성)
     * @param at                값의 시각
     * @param reportIntervalSec 그 기기의 보고 주기(없으면 null)
     * @param eventDriven       이벤트형 센서(바뀔 때만 보고)
     * @param offline           기기가 OFFLINE
     */
    public record Input(Object value, Instant at, Integer reportIntervalSec, boolean eventDriven, boolean offline) {
    }

    /** 판정 결과 */
    public record Decision(Interlock interlock, Reason reason) {
        public static final Decision PASS = new Decision(null, null);

        public boolean blocked() {
            return interlock != null;
        }

        /** 요청자에게 돌려줄 사유 */
        public String message() {
            if (interlock == null) {
                return null;
            }
            String base = interlock.message() == null ? interlock.name() : interlock.message();
            return reason == Reason.STALE_DATA ? "조건 데이터 없음: " + base : base;
        }
    }

    public enum Reason { CONDITION_TRUE, STALE_DATA }

    private InterlockEvaluator() {
    }

    /**
     * @param interlocks 기기에 걸리는 규칙
     * @param inputs     조건 → 입력들(관계 조건이면 여러 기기). 데이터가 하나도 없으면 빈 목록
     */
    public static Decision evaluate(List<Interlock> interlocks, String capability, String command, Map<String, ?> args,
                                    Function<Interlock.Condition, List<Optional<Input>>> inputs, Instant now) {
        for (Interlock il : interlocks) {
            if (il.forbid() == null || il.condition() == null || !il.forbid().matches(capability, command, args)) {
                continue;
            }
            List<Optional<Input>> values = inputs.apply(il.condition());
            if (values.isEmpty()) {
                return new Decision(il, Reason.STALE_DATA);
            }
            for (Optional<Input> v : values) {
                if (v.isEmpty() || stale(il, v.get(), now)) {
                    return new Decision(il, Reason.STALE_DATA);
                }
                if (Interlock.Comparison.compare(v.get().value(), il.condition().op(), il.condition().value())) {
                    return new Decision(il, Reason.CONDITION_TRUE);
                }
            }
        }
        return Decision.PASS;
    }

    /** 데이터가 너무 오래되었는가 */
    static boolean stale(Interlock il, Input in, Instant now) {
        if (in.eventDriven()) {
            return in.offline();
        }
        if (in.at() == null || in.value() == null) {
            return true;
        }
        return Duration.between(in.at(), now).compareTo(staleAfter(il, in.reportIntervalSec())) > 0;
    }

    /** 데이터 없음 판정 시간: max(5분, 보고 주기 × 3, 규칙 설정) */
    public static Duration staleAfter(Interlock il, Integer reportIntervalSec) {
        Duration d = MIN_STALE;
        if (reportIntervalSec != null && reportIntervalSec > 0) {
            Duration byReport = Duration.ofSeconds(reportIntervalSec * 3L);
            d = byReport.compareTo(d) > 0 ? byReport : d;
        }
        if (il.staleAfterSec() != null && il.staleAfterSec() > 0) {
            Duration configured = Duration.ofSeconds(il.staleAfterSec());
            d = configured.compareTo(d) > 0 ? configured : d;
        }
        return d;
    }
}
