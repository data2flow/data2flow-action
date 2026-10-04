package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.contracts.secret.Secret;

import java.util.Map;

/**
 * 드라이버 설정(core {@code drivers}의 type·config, 비밀값 제외).
 *
 * @param driverId 드라이버 ID(저장 전 연결 확인이면 null)
 * @param type     종류
 * @param config   종류별 연결 정보
 * @param secrets  비밀값(LoRaWAN apiToken, 벤더 token 등). core가 복호화해 API-ACT-40·연결 확인 본문으로 준다(ADR-049)
 */
public record DriverConfig(Long driverId, String type, Map<String, Object> config, Map<String, Secret> secrets) {

    public DriverConfig {
        config = config == null ? Map.of() : Map.copyOf(config);
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
    }

    /** 비밀값 없는 설정(M3 모양) */
    public DriverConfig(Long driverId, String type, Map<String, Object> config) {
        this(driverId, type, config, null);
    }

    /** 비밀값(API 토큰 등). 없으면 null. {@link Secret}은 출력·직렬화에서 언제나 가려진다 */
    public Secret secret(String name) {
        return secrets.get(name);
    }

    /** 문자열 설정 값. 없으면 {@code fallback} */
    public String string(String key, String fallback) {
        Object v = config.get(key);
        return v == null ? fallback : v.toString();
    }
}
