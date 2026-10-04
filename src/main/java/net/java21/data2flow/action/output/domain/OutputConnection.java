package net.java21.data2flow.action.output.domain;

import net.java21.data2flow.contracts.output.OutputConnectionType;
import net.java21.data2flow.contracts.output.OutputFilter;
import net.java21.data2flow.contracts.output.OutputFormat;
import net.java21.data2flow.contracts.output.OutputTopicTemplate;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 출력 연결 정의(core API-DSC-73 항목·API-DSC-76 본문, DSC-04.01). 비밀값은 복호화된 원문이라 로그·toString에 남기지 않는다.
 *
 * @param secrets 종류(PASSWORD·CA_CERT·HEADER_VALUE·HMAC_KEY) → 원문
 */
public record OutputConnection(long id, long organizationId, String name, OutputConnectionType type, JsonNode target, OutputFilter filter,
                               OutputFormat format, String template, Map<String, String> secrets, boolean enabled, long version) {

    public OutputConnection {
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
        filter = filter == null ? OutputFilter.all() : filter;
        format = format == null ? OutputFormat.CANONICAL : format;
    }

    /** API-DSC-73 항목·API-DSC-76 본문 모양에서 읽는다(ID는 문자열·숫자 모두) */
    public static OutputConnection parse(JsonNode n) {
        Map<String, String> secrets = new LinkedHashMap<>();
        n.path("secrets").properties().forEach(e -> {
            if (e.getValue() != null && !e.getValue().isNull()) {
                secrets.put(e.getKey(), e.getValue().asString());
            }
        });
        return new OutputConnection(id(n.path("id")), id(n.path("organizationId")), n.path("name").asString(""),
                type(n.path("type").asString("")), n.path("target"), filter(n.path("filter")), format(n.path("format").asString("")),
                n.path("template").isNull() ? null : n.path("template").asString(null), secrets, n.path("enabled").asBoolean(true),
                n.path("version").asLong(0));
    }

    static OutputConnectionType type(String raw) {
        try {
            return OutputConnectionType.valueOf(raw.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return OutputConnectionType.UNKNOWN;
        }
    }

    static OutputFormat format(String raw) {
        if (raw == null || raw.isBlank()) {
            return OutputFormat.CANONICAL;
        }
        try {
            return OutputFormat.valueOf(raw.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return OutputFormat.UNKNOWN;
        }
    }

    static OutputFilter filter(JsonNode f) {
        if (f == null || f.isMissingNode() || f.isNull()) {
            return OutputFilter.all();
        }
        Integer quality = f.path("qualityMin").isNumber() ? f.path("qualityMin").asInt() : null;
        return new OutputFilter(ids(f.path("deviceIds")), ids(f.path("groupIds")), ids(f.path("spaceIds")), texts(f.path("metrics")),
                quality);
    }

    static long id(JsonNode n) {
        if (n == null || n.isMissingNode() || n.isNull()) {
            return 0;
        }
        return n.isNumber() ? n.asLong() : Long.parseLong(n.asString().strip());
    }

    static List<Long> ids(JsonNode arr) {
        List<Long> out = new ArrayList<>();
        arr.forEach(x -> out.add(id(x)));
        return out;
    }

    static List<String> texts(JsonNode arr) {
        List<String> out = new ArrayList<>();
        arr.forEach(x -> out.add(x.asString()));
        return out;
    }

    /** MQTT 토픽 템플릿(검사 통과한 것만, 아니면 IllegalArgumentException) */
    public OutputTopicTemplate topicTemplate() {
        return OutputTopicTemplate.of(target.path("topicTemplate").asString(""));
    }

    public String url() {
        return target.path("url").asString("");
    }

    @Override
    public String toString() {
        return "OutputConnection[id=" + id + ", org=" + organizationId + ", type=" + type + ", format=" + format + ", enabled=" + enabled
                + ", version=" + version + ", secrets=" + secrets.keySet() + "]";
    }
}
