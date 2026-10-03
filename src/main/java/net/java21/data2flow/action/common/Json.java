package net.java21.data2flow.action.common;

import net.java21.data2flow.contracts.message.MessageCodec;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/** JSON 변환(메시지 계약과 같은 설정: ISO-8601 시각, 모르는 필드 무시) */
public final class Json {

    public static final JsonMapper MAPPER = MessageCodec.newMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };
    private static final TypeReference<Map<String, Map<String, Object>>> STATE = new TypeReference<>() {
    };

    private Json() {
    }

    public static String write(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    public static <T> T read(String json, Class<T> type) {
        return json == null ? null : MAPPER.readValue(json, type);
    }

    public static Map<String, Object> map(String json) {
        return json == null ? Map.of() : MAPPER.readValue(json, MAP);
    }

    /** {@code {capability:{attr:value}}} */
    public static Map<String, Map<String, Object>> state(String json) {
        return json == null ? Map.of() : MAPPER.readValue(json, STATE);
    }

    public static Map<String, Object> toMap(Object value) {
        return MAPPER.convertValue(value, MAP);
    }
}
