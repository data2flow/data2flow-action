package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 장비 보호 기본값(ACT-06.01, BR-ACT-10): 압축기 장비의 최소 꺼짐 3분·최소 켜짐 5분, 하루 최대 켜기 횟수.
 *
 * @param minOffSec       끈 뒤 다시 켜기까지 최소 시간(초). 없으면 검사 안 함
 * @param minOnSec        켠 뒤 끄기까지 최소 시간(초). 없으면 검사 안 함
 * @param maxCyclesPerDay 하루(UTC 날짜) 최대 켜기 횟수. 없으면 검사 안 함
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Protection(Integer minOffSec, Integer minOnSec, Integer maxCyclesPerDay) {

    /** 압축기 장비 기본값(ACT domain-model model_capabilities.protection) */
    public static final Protection COMPRESSOR = new Protection(180, 300, 20);
}
