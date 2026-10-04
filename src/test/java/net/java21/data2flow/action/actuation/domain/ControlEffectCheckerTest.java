package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.capability.ExpectedEffect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 제어 효과 확인(ACT-08.01, BR-ACT-20) */
class ControlEffectCheckerTest {

    @ParameterizedTest(name = "{0} 시작 {1} → 끝 {2} → {3}")
    @CsvSource({
            "DOWN, 27.0, 26.5, EFFECTIVE",
            "DOWN, 27.0, 27.1, NO_EFFECT",     // 냉방인데 +0.1℃
            "DOWN, 27.0, 26.9, NO_EFFECT",     // 최소 변화 0.2 미만
            "DOWN, 27.0, 26.8, EFFECTIVE",     // 경계: -0.2
            "UP, 18.0, 18.5, EFFECTIVE",
            "UP, 18.0, 17.0, NO_EFFECT"
    })
    @DisplayName("[ACT-08.01][TC-ACT-134] BR-ACT-20 규칙 표: 기대 방향으로 최소 변화(0.2) 이상이면 효과 있음")
    void judge(ExpectedEffect.Direction direction, double start, double end, ControlEffects.Verdict verdict) {
        assertThat(ControlEffects.judge(direction, start, end, 0.2)).isEqualTo(verdict);
    }

    @Test
    @DisplayName("[ACT-08.01][TC-ACT-134] 측정값이 없으면 판정하지 않는다(UNKNOWN), 기대 효과는 명령·인자가 맞는 것만")
    void expectedEffects() {
        assertThat(ControlEffects.judge(ExpectedEffect.Direction.DOWN, null, 26.0, 0.2)).isEqualTo(ControlEffects.Verdict.UNKNOWN);
        CapabilityCatalog catalog = CapabilityCatalog.standard();
        var cool = ControlEffects.expected(catalog, "Thermostat", "set", Map.of("mode", "cool", "targetTemperature", 24));
        assertThat(cool).isPresent();
        assertThat(cool.get().metric()).isEqualTo("temperature");
        assertThat(cool.get().direction()).isEqualTo(ExpectedEffect.Direction.DOWN);
        assertThat(cool.get().withinMinutes()).isEqualTo(15);
        assertThat(ControlEffects.expected(catalog, "Thermostat", "set", Map.of("targetTemperature", 24))).isEmpty();
        assertThat(ControlEffects.expected(catalog, "Switch", "set", Map.of("on", true))).isEmpty();
        assertThat(ControlEffects.expected(catalog, "NoSuch", "set", Map.of())).isEmpty();
    }
}
