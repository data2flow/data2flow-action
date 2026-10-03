package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import net.java21.data2flow.contracts.capability.AttributeConstraint;

import java.util.Map;

/**
 * 조직 제어 설정(core {@code control_settings}, API-ACT-17). 값이 없으면 ACT domain-model의 기본값.
 *
 * @param absoluteLimits                기능 → 속성 → 절대 한계(ACT-06.04)
 * @param manualOverrideMinutes         수동 우선 시간(기본 30, 0=끔, BR-ACT-08)
 * @param minIntervalSec                같은 기기·기능 자동 명령 최소 간격(기본 10, BR-ACT-07)
 * @param oscillation                   진동 판정(기본 60초 안 3번째)
 * @param defaultValiditySec            유효 시간 기본값(600)
 * @param scheduleRespectsManualOverride 수동 우선 중 예약 명령을 막는지(기본 true)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ControlSettings(Map<String, Map<String, AttributeConstraint>> absoluteLimits, Integer manualOverrideMinutes,
                              Integer minIntervalSec, Oscillation oscillation, Integer defaultValiditySec,
                              Boolean scheduleRespectsManualOverride) {

    public static final ControlSettings DEFAULT = new ControlSettings(null, null, null, null, null, null);

    public ControlSettings {
        absoluteLimits = absoluteLimits == null ? Map.of() : Map.copyOf(absoluteLimits);
        manualOverrideMinutes = manualOverrideMinutes == null || manualOverrideMinutes < 0 ? 30 : manualOverrideMinutes;
        minIntervalSec = minIntervalSec == null || minIntervalSec < 0 ? 10 : minIntervalSec;
        oscillation = oscillation == null ? Oscillation.DEFAULT : oscillation;
        defaultValiditySec = defaultValiditySec == null || defaultValiditySec <= 0 ? 600 : defaultValiditySec;
        scheduleRespectsManualOverride = scheduleRespectsManualOverride == null || scheduleRespectsManualOverride;
    }

    /** 이 기능의 절대 한계(없으면 빈 맵) */
    public Map<String, AttributeConstraint> limitsFor(String capability) {
        return absoluteLimits.getOrDefault(capability, Map.of());
    }

    /**
     * @param windowSec 창(초)
     * @param flips     창 안에서 반대 명령이 이어진 횟수(명령 수) 한도. 기본 3(on→off→on의 세 번째를 막음)
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Oscillation(int windowSec, int flips) {
        public static final Oscillation DEFAULT = new Oscillation(60, 3);

        public Oscillation {
            windowSec = windowSec <= 0 ? 60 : windowSec;
            flips = flips < 2 ? 3 : flips;
        }
    }
}
