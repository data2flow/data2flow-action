package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * 인터락 규칙(core {@code interlock_rules}, API-ACT-16, ACT domain-model). 기기에 걸리는 규칙만 core가 골라 준다(API-ACT-44).
 *
 * @param interlockId     규칙 ID
 * @param name            이름
 * @param spaceId         규칙 공간
 * @param includeChildren 하위 공간 포함
 * @param condition       조건(참이면 막는다)
 * @param forbid          금지 대상
 * @param message         요청자에게 돌려줄 사유
 * @param staleAfterSec   "조건 데이터 없음" 판정 시간(초). 기본(보고 주기 × 3, 최소 5분)보다 길게만 쓴다(BR-ACT-11). 없으면 null
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Interlock(Long interlockId, String name, Long spaceId, Boolean includeChildren, Condition condition, Forbid forbid,
                        String message, Integer staleAfterSec) {

    /**
     * 조건. {@code kind=metric}: 측정값({@code deviceId} 또는 {@code spaceAgg} 공간 평균), {@code kind=state}: 기기 상태
     * ({@code deviceId} 또는 {@code relation} 관계 기기 중 하나라도).
     *
     * @param kind      metric | state
     * @param deviceId  조건 기기
     * @param spaceAgg  공간 집계(true면 규칙 공간의 평균)
     * @param relation  관계(measures·controls)로 고른 기기들
     * @param metric    측정 항목(metric)
     * @param capability 기능(state)
     * @param attribute  속성(state)
     * @param op        비교(&gt;, &gt;=, &lt;, &lt;=, ==, !=)
     * @param value     기준 값
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Condition(String kind, Long deviceId, Boolean spaceAgg, String relation, String metric, String capability,
                            String attribute, String op, Object value) {

        public boolean state() {
            return "state".equalsIgnoreCase(kind);
        }
    }

    /**
     * 금지 대상.
     *
     * @param capability 기능
     * @param command    명령(없으면 모든 명령)
     * @param argsMatch  인자 조건 {@code {mode:{in:[cool, heat]}}}·{@code {on:{eq:true}}}·{@code {level:{gte:2}}}. 없으면 모든 인자
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Forbid(String capability, String command, Map<String, Map<String, Object>> argsMatch) {

        public Forbid {
            argsMatch = argsMatch == null ? Map.of() : Map.copyOf(argsMatch);
        }

        /** 이 명령이 금지 대상인가 */
        public boolean matches(String cap, String cmd, Map<String, ?> args) {
            if (capability == null || !capability.equals(cap) || (command != null && !command.equals(cmd))) {
                return false;
            }
            for (Map.Entry<String, Map<String, Object>> e : argsMatch.entrySet()) {
                Object actual = args == null ? null : args.get(e.getKey());
                if (actual == null || !Comparison.matchesAll(actual, e.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }

    /** 값 비교(인자 조건·조건 연산) */
    public static final class Comparison {

        private Comparison() {
        }

        static boolean matchesAll(Object actual, Map<String, Object> spec) {
            for (Map.Entry<String, Object> c : spec.entrySet()) {
                boolean ok = switch (c.getKey()) {
                    case "in" -> c.getValue() instanceof List<?> l && l.stream().anyMatch(v -> same(actual, v));
                    case "notIn" -> c.getValue() instanceof List<?> l && l.stream().noneMatch(v -> same(actual, v));
                    case "eq" -> same(actual, c.getValue());
                    case "ne" -> !same(actual, c.getValue());
                    case "gt" -> compare(actual, ">", c.getValue());
                    case "gte" -> compare(actual, ">=", c.getValue());
                    case "lt" -> compare(actual, "<", c.getValue());
                    case "lte" -> compare(actual, "<=", c.getValue());
                    default -> false;
                };
                if (!ok) {
                    return false;
                }
            }
            return true;
        }

        /** 조건 연산. 숫자는 값으로, 그 밖은 같음·다름만 */
        public static boolean compare(Object actual, String op, Object expected) {
            if (actual == null || op == null) {
                return false;
            }
            if (actual instanceof Number a && expected instanceof Number b) {
                int cmp = Double.compare(a.doubleValue(), b.doubleValue());
                return switch (op) {
                    case ">" -> cmp > 0;
                    case ">=" -> cmp >= 0;
                    case "<" -> cmp < 0;
                    case "<=" -> cmp <= 0;
                    case "==", "=" -> cmp == 0;
                    case "!=" -> cmp != 0;
                    default -> false;
                };
            }
            return switch (op) {
                case "==", "=" -> same(actual, expected);
                case "!=" -> !same(actual, expected);
                default -> false;
            };
        }

        static boolean same(Object a, Object b) {
            if (a instanceof Number x && b instanceof Number y) {
                return Double.compare(x.doubleValue(), y.doubleValue()) == 0;
            }
            return a != null && b != null && a.toString().equals(b.toString());
        }
    }
}
