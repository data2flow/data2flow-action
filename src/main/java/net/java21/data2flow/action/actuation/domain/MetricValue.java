package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;

/**
 * 측정값 하나(core API-ACT-45 {@code GET /internal/core/metric-values}): 인터락 측정 조건(BR-ACT-11)과 제어 효과 확인(BR-ACT-20)이 쓴다.
 *
 * @param metric            측정 항목
 * @param value             값(공간이면 측정 기기 평균)
 * @param measuredAt        측정 시각
 * @param reportIntervalSec 측정 기기 보고 주기. 없으면 null
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MetricValue(String metric, Double value, Instant measuredAt, Integer reportIntervalSec) {
}
