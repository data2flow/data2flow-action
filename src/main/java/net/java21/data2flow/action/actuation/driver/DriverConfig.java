package net.java21.data2flow.action.actuation.driver;

import java.util.Map;

/**
 * 드라이버 설정(core {@code drivers}의 type·config, 비밀값 제외).
 *
 * @param driverId 드라이버 ID(저장 전 연결 확인이면 null)
 * @param type     종류
 * @param config   종류별 연결 정보
 */
public record DriverConfig(Long driverId, String type, Map<String, Object> config) {

    public DriverConfig {
        config = config == null ? Map.of() : Map.copyOf(config);
    }

    /** 문자열 설정 값. 없으면 {@code fallback} */
    public String string(String key, String fallback) {
        Object v = config.get(key);
        return v == null ? fallback : v.toString();
    }
}
