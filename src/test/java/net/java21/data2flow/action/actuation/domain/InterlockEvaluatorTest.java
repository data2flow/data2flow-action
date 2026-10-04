package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.action.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** 인터락 판정(ACT-06.02, BR-ACT-11) */
class InterlockEvaluatorTest {

    private static final Instant NOW = MutableClock.T0;

    /** 창문(Contact 기기 21)이 열려 있으면 냉방·난방 금지 */
    static Interlock windowOpen() {
        return new Interlock(5L, "창문 열림 냉난방 금지", 31L, true,
                new Interlock.Condition("state", 21L, null, null, null, "Contact", "open", "==", true),
                new Interlock.Forbid("Thermostat", "set", Map.of("mode", Map.of("in", List.of("cool", "heat")))),
                "창문이 열려 있어 냉난방을 켤 수 없습니다", null);
    }

    /** 실내 CO2(공간 평균) 2000ppm 넘으면 환기 끄기 금지 */
    static Interlock co2() {
        return new Interlock(6L, "고농도 CO2 환기 유지", 31L, true,
                new Interlock.Condition("metric", null, true, null, "co2", null, null, ">", 2000),
                new Interlock.Forbid("Ventilation", null, Map.of("mode", Map.of("eq", "off"))), "CO2가 높아 환기를 끌 수 없습니다", null);
    }

    private static InterlockEvaluator.Decision eval(Interlock il, String cap, Map<String, ?> args, Optional<InterlockEvaluator.Input> in) {
        return InterlockEvaluator.evaluate(List.of(il), cap, "set", args, c -> List.of(in), NOW);
    }

    @ParameterizedTest(name = "{0} {1} 창문 open={2} → 차단={3}")
    @CsvSource({
            "Thermostat, cool, true, true",
            "Thermostat, heat, true, true",
            "Thermostat, fan, true, false",     // 금지 대상 아님(송풍)
            "Thermostat, cool, false, false",   // 조건 거짓
            "Switch, cool, true, false"         // 다른 기능
    })
    @DisplayName("[ACT-06.02][TC-ACT-100] BR-ACT-11 규칙 표: 조건 참 + 금지 대상이면 BLOCKED(INTERLOCK)")
    void table(String capability, String mode, boolean open, boolean blocked) {
        var in = Optional.of(new InterlockEvaluator.Input(open, NOW.minusSeconds(60), null, true, false));
        var d = eval(windowOpen(), capability, Map.of("mode", mode), in);
        assertThat(d.blocked()).isEqualTo(blocked);
        if (blocked) {
            assertThat(d.message()).isEqualTo("창문이 열려 있어 냉난방을 켤 수 없습니다");
            assertThat(d.reason()).isEqualTo(InterlockEvaluator.Reason.CONDITION_TRUE);
        }
    }

    @Test
    @DisplayName("[ACT-06.02][AT-ACT-08.2][TC-ACT-101] 측정 조건: 5분 1초 넘게 데이터 없음 → 안전 쪽 차단, 4분 59초면 마지막 값으로 판단")
    void staleMetric() {
        Map<String, Object> off = Map.of("mode", "off");
        var fresh = Optional.of(new InterlockEvaluator.Input(1500.0, NOW.minus(Duration.ofSeconds(299)), null, false, false));
        assertThat(eval(co2(), "Ventilation", off, fresh).blocked()).isFalse();
        var stale = Optional.of(new InterlockEvaluator.Input(1500.0, NOW.minus(Duration.ofSeconds(301)), null, false, false));
        var d = eval(co2(), "Ventilation", off, stale);
        assertThat(d.blocked()).isTrue();
        assertThat(d.reason()).isEqualTo(InterlockEvaluator.Reason.STALE_DATA);
        assertThat(d.message()).startsWith("조건 데이터 없음");
        var high = Optional.of(new InterlockEvaluator.Input(2100.0, NOW.minusSeconds(10), null, false, false));
        assertThat(eval(co2(), "Ventilation", off, high).blocked()).isTrue();
        assertThat(eval(co2(), "Ventilation", Map.of("mode", "on"), high).blocked()).isFalse();
        // 값이 하나도 없으면 안전 쪽
        assertThat(InterlockEvaluator.evaluate(List.of(co2()), "Ventilation", "set", off, c -> List.of(), NOW).blocked()).isTrue();
        assertThat(eval(co2(), "Ventilation", off, Optional.empty()).blocked()).isTrue();
    }

    @Test
    @DisplayName("[ACT-06.02] 비교 연산과 인자 조건")
    void comparisons() {
        assertThat(Interlock.Comparison.compare(3, ">=", 3)).isTrue();
        assertThat(Interlock.Comparison.compare(2, "<", 3)).isTrue();
        assertThat(Interlock.Comparison.compare(2, "<=", 1)).isFalse();
        assertThat(Interlock.Comparison.compare(2, "!=", 3)).isTrue();
        assertThat(Interlock.Comparison.compare("a", "==", "a")).isTrue();
        assertThat(Interlock.Comparison.compare("a", "!=", "b")).isTrue();
        assertThat(Interlock.Comparison.compare("a", ">", "b")).isFalse();
        assertThat(Interlock.Comparison.compare(null, "==", 1)).isFalse();
        assertThat(Interlock.Comparison.compare(1, "~", 1)).isFalse();
        var f = new Interlock.Forbid("FanSpeed", null, Map.of("level", Map.of("gte", 2, "lt", 4)));
        assertThat(f.matches("FanSpeed", "set", Map.of("level", 3))).isTrue();
        assertThat(f.matches("FanSpeed", "set", Map.of("level", 4))).isFalse();
        assertThat(f.matches("FanSpeed", "set", Map.of())).isFalse();
        var g = new Interlock.Forbid("Lock", "set", Map.of("locked", Map.of("ne", true, "notIn", List.of(true))));
        assertThat(g.matches("Lock", "set", Map.of("locked", false))).isTrue();
        assertThat(g.matches("Lock", "other", Map.of("locked", false))).isFalse();
        assertThat(new Interlock.Forbid("Lock", null, Map.of("locked", Map.of("weird", 1))).matches("Lock", "set", Map.of("locked", true)))
                .isFalse();
        assertThat(new Interlock.Forbid("Dimmer", null, Map.of("level", Map.of("gt", 50, "lte", 80))).matches("Dimmer", "set",
                Map.of("level", 70))).isTrue();
    }
}
