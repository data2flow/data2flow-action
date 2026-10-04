package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Duration;

/**
 * 드라이버 서킷 브레이커 설정(core {@code drivers.circuit} {@code {failureRate:50, windowSec:60, openSec:30}}, BR-ACT-14).
 *
 * @param failureRate 열림 기준 실패율(%)
 * @param windowSec   실패율을 보는 창(초)
 * @param openSec     열린 뒤 즉시 실패시키는 시간(초). 지나면 시험 호출 1건(HALF_OPEN)
 * @param minCalls    판정에 필요한 최소 호출 수(호출이 적을 때 한 번 실패로 열리지 않게)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CircuitPolicy(double failureRate, int windowSec, int openSec, int minCalls) {

    public static final CircuitPolicy DEFAULT = new CircuitPolicy(50, 60, 30, 5);

    public CircuitPolicy {
        failureRate = failureRate <= 0 || failureRate > 100 ? 50 : failureRate;
        windowSec = windowSec <= 0 ? 60 : windowSec;
        openSec = openSec <= 0 ? 30 : openSec;
        minCalls = minCalls <= 0 ? 5 : minCalls;
    }

    public Duration window() {
        return Duration.ofSeconds(windowSec);
    }

    public Duration open() {
        return Duration.ofSeconds(openSec);
    }
}
