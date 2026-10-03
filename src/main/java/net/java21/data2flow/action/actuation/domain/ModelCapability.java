package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import net.java21.data2flow.contracts.capability.AttributeConstraint;

import java.util.Map;

/**
 * 모델의 기능 하나(core {@code model_capabilities}, ACT-01.03·06.01).
 *
 * @param constraints        모델 제약 속성 → 범위·허용 값(예: targetTemperature 18~30)
 * @param protection         장비 보호(압축기 장비의 최소 꺼짐·켜짐, 하루 최대 반복). 없으면 null
 * @param reapplyOnReconnect 재연결 시 desired 재적용(BR-ACT-06)
 * @param classADownlink     LoRaWAN Class A(M4, ACT-07.02)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ModelCapability(Map<String, AttributeConstraint> constraints, Protection protection, boolean reapplyOnReconnect,
                              boolean classADownlink) {

    public static final ModelCapability PLAIN = new ModelCapability(Map.of(), null, false, false);

    public ModelCapability {
        constraints = constraints == null ? Map.of() : Map.copyOf(constraints);
    }
}
