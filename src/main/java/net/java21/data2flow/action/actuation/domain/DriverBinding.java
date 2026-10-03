package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Duration;
import java.util.Map;

/**
 * 모델에 연결된 드라이버(core {@code drivers} + {@code driver_bindings}, API-ACT-30·31). 비밀값은 싣지 않는다.
 *
 * @param driverId        드라이버 ID
 * @param type            종류(VIRTUAL·MQTT·LORAWAN·LG_THINQ·SMARTTHINGS)
 * @param config          종류별 연결 정보(비밀값 제외)
 * @param ackTimeoutSec   ack 기한(기본 30)
 * @param applyTimeoutSec 적용 기한(기본 60)
 * @param retry           재시도 정책. 없으면 기본(최대 3회, 1초 시작, 2배, 최대 10초)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DriverBinding(Long driverId, String type, Map<String, Object> config, Integer ackTimeoutSec,
                            Integer applyTimeoutSec, RetryPolicy retry) {

    public DriverBinding {
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    public Duration ackTimeout(Duration fallback) {
        return ackTimeoutSec == null || ackTimeoutSec <= 0 ? fallback : Duration.ofSeconds(ackTimeoutSec);
    }

    public Duration applyTimeout(Duration fallback) {
        return applyTimeoutSec == null || applyTimeoutSec <= 0 ? fallback : Duration.ofSeconds(applyTimeoutSec);
    }
}
