package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.contracts.secret.Secret;

import java.util.Map;

/**
 * Sink 연결 하나(core {@code sink_connections}, API-FLW-85). 비밀값은 {@link Secret}으로만 들고 다닌다(로그·직렬화에 가려짐).
 *
 * @param connectionId   연결 ID(저장 전 연결 테스트면 0)
 * @param organizationId 조직
 * @param type           종류
 * @param config         접속 정보(host, port, database, username, schema, ssl, url, org, bucket, precision)
 * @param secrets        비밀값(password·token)
 * @param version        정의 버전(바뀌면 풀을 다시 만든다)
 */
public record SinkConnection(long connectionId, long organizationId, String type, Map<String, Object> config,
                             Map<String, Secret> secrets, long version) {

    public SinkConnection {
        config = config == null ? Map.of() : Map.copyOf(config);
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
    }

    public String string(String key, String fallback) {
        Object v = config.get(key);
        return v == null || v.toString().isBlank() ? fallback : v.toString();
    }

    public int integer(String key, int fallback) {
        Object v = config.get(key);
        if (v instanceof Number n) {
            return n.intValue();
        }
        try {
            return v == null ? fallback : Integer.parseInt(v.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    public boolean flag(String key) {
        Object v = config.get(key);
        return v instanceof Boolean b ? b : v != null && Boolean.parseBoolean(v.toString());
    }

    /** 비밀값 평문. 없으면 null */
    public String secret(String name) {
        Secret s = secrets.get(name);
        return s == null ? null : s.reveal();
    }

    /** 풀 키(연결 ID + 버전) */
    public String poolKey() {
        return connectionId + ":" + version + ":" + Integer.toHexString(config.hashCode());
    }
}
