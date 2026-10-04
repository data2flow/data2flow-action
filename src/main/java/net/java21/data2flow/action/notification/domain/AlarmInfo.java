package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.alarm.AlarmStatus;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 알림 판단에 쓰는 알람 정보(core API-RUL-41 = contracts {@code AlarmSnapshot} 모양 + {@code space.pathIds}).
 *
 * @param spacePathIds 루트부터 알람 공간까지 ID. core가 {@code pathIds}를 주지 않으면 {@code space.path}("/1/7/31")에서, 그것도 없으면 공간 하나
 */
public record AlarmInfo(long id, String alarmKey, AlarmSeverity severity, AlarmStatus status, boolean flapping, String suppressedReason,
                        String title, Long ruleId, Long deviceId, String deviceName, Long spaceId, String spacePath,
                        List<Long> spacePathIds, String metric, Double value, int occurrenceCount) {

    public AlarmInfo {
        spacePathIds = spacePathIds == null ? List.of() : List.copyOf(spacePathIds);
    }

    public static AlarmInfo from(JsonNode a) {
        JsonNode space = a.path("space");
        Long spaceId = space.isMissingNode() || space.isNull() ? null : space.path("id").asLong();
        List<Long> path = new ArrayList<>();
        space.path("pathIds").forEach(n -> path.add(n.asLong()));
        String spacePath = space.path("path").isString() ? space.path("path").asString() : null;
        if (path.isEmpty() && spacePath != null) {
            for (String part : spacePath.split("/")) {
                if (part.matches("\\d+")) {
                    path.add(Long.parseLong(part));
                }
            }
        }
        if (path.isEmpty() && spaceId != null) {
            path.add(spaceId);
        }
        JsonNode device = a.path("device");
        Double value = a.path("lastValue").isNumber() ? a.path("lastValue").asDouble()
                : a.path("triggerValue").isNumber() ? a.path("triggerValue").asDouble() : null;
        return new AlarmInfo(a.path("id").asLong(), text(a, "alarmKey"), severity(text(a, "severity")), status(text(a, "status")),
                a.path("flapping").asBoolean(false), text(a, "suppressedReason"), text(a, "title"),
                a.path("source").path("ruleId").isMissingNode() || a.path("source").path("ruleId").isNull() ? null
                        : a.path("source").path("ruleId").asLong(),
                device.isMissingNode() || device.isNull() ? null : device.path("id").asLong(),
                device.isMissingNode() || device.isNull() ? null : text(device, "name"),
                spaceId, spacePath, path, text(a, "metric"), value, a.path("occurrenceCount").asInt(1));
    }

    /** 템플릿 변수(요청 변수보다 우선순위가 낮다) */
    public Map<String, Object> variables() {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("alarm", Map.of("id", id, "severity", severity.name(), "status", status.name(), "count", occurrenceCount));
        v.put("title", title == null ? "" : title);
        v.put("severity", severity.name());
        if (deviceName != null) {
            v.put("device", Map.of("id", deviceId, "name", deviceName));
        }
        if (spacePath != null) {
            v.put("space", spacePath);
        }
        if (metric != null) {
            v.put("metric", metric);
        }
        if (value != null) {
            v.put("value", value);
        }
        return v;
    }

    private static String text(JsonNode n, String field) {
        JsonNode v = n.path(field);
        return v.isMissingNode() || v.isNull() ? null : v.asString();
    }

    private static AlarmSeverity severity(String s) {
        try {
            return s == null ? AlarmSeverity.UNKNOWN : AlarmSeverity.valueOf(s);
        } catch (IllegalArgumentException e) {
            return AlarmSeverity.UNKNOWN;
        }
    }

    private static AlarmStatus status(String s) {
        try {
            return s == null ? AlarmStatus.UNKNOWN : AlarmStatus.valueOf(s);
        } catch (IllegalArgumentException e) {
            return AlarmStatus.UNKNOWN;
        }
    }
}
