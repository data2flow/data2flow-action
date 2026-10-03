package net.java21.data2flow.action.actuation.domain;

import java.util.Map;
import java.util.Optional;

/**
 * 기능의 "켜짐/꺼짐"(장비 보호 BR-ACT-10, 진동 판정 BR-ACT-07). Switch는 {@code on}, Thermostat·Ventilation은 {@code mode != off}.
 * 켜짐/꺼짐을 바꾸지 않는 인자(예: 목표 온도만)는 빈 값이다.
 */
public final class PowerState {

    private PowerState() {
    }

    /** 명령 인자가 정하는 전원 목표. 전원과 관계없으면 빈 값 */
    public static Optional<Boolean> target(String capability, Map<String, ?> args) {
        if (args == null) {
            return Optional.empty();
        }
        return switch (capability) {
            case "Switch" -> args.get("on") instanceof Boolean b ? Optional.of(b) : Optional.empty();
            case "Thermostat", "Ventilation" -> args.get("mode") instanceof String m ? Optional.of(!"off".equals(m)) : Optional.empty();
            default -> Optional.empty();
        };
    }

    /** 현재 상태(속성 맵)의 전원. 모르면 빈 값 */
    public static Optional<Boolean> current(String capability, Map<String, ?> attributes) {
        return target(capability, attributes);
    }
}
