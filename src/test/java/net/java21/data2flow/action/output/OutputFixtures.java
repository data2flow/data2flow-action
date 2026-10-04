package net.java21.data2flow.action.output;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.domain.DeviceContext;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.Quality;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 출력 연결 시험 데이터(TC-DSC-126 "OutputConnectionFixtures"): 실습실 AM107 텔레메트리, 연결 정의(API-DSC-73 모양), 기기 맥락 */
public final class OutputFixtures {

    public static final long ORG = 1;
    public static final long DEVICE = 501;

    private OutputFixtures() {
    }

    /** 실습실 AM107: temperature·humidity·co2 */
    public static CanonicalTelemetry am107(Instant at, double co2) {
        return CanonicalTelemetry.builder().messageId(UUID.randomUUID()).organizationId(ORG).sourceId(10).externalId("24e124710c408089")
                .deviceId(DEVICE).deviceStatus(CanonicalTelemetry.DeviceStatus.ACTIVE).modelId("AM107").spaceId(31L)
                .measuredAt(at).receivedAt(at).rawMessageId(9001)
                .metric(CanonicalTelemetry.Metric.of("temperature", 23.5, "°C"))
                .metric(CanonicalTelemetry.Metric.of("humidity", 41.0, "%"))
                .metric(new CanonicalTelemetry.Metric("co2", co2, "ppm", Quality.NORMAL, null))
                .build();
    }

    public static DeviceContext context() {
        return new DeviceContext(DEVICE, ORG, "am107-lab", 31L, "R101", List.of(1L, 30L, 31L), Set.of(7L));
    }

    /** API-DSC-74 응답 항목 */
    public static Map<String, Object> contextJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("deviceId", Long.toString(DEVICE));
        m.put("organizationId", Long.toString(ORG));
        m.put("deviceName", "am107-lab");
        m.put("spaceId", "31");
        m.put("spaceCode", "R101");
        m.put("spacePathIds", List.of("1", "30", "31"));
        m.put("groupIds", List.of("7"));
        return m;
    }

    /** MQTT 출력 연결(API-DSC-73 항목) */
    public static Map<String, Object> mqtt(long id, String url, String topicTemplate, Map<String, Object> filter) {
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("url", url);
        target.put("topicTemplate", topicTemplate);
        target.put("qos", 1);
        return connection(id, "MQTT_PUBLISH", target, filter, "CANONICAL", null, Map.of());
    }

    /** Webhook 출력 연결 */
    public static Map<String, Object> webhook(long id, String url, Map<String, Object> filter, Map<String, Object> extraTarget,
                                              Map<String, String> secrets) {
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("url", url);
        target.put("method", "POST");
        target.put("batchSize", 100);
        target.put("batchWaitMs", 0);
        target.put("timeoutMs", 2000);
        target.putAll(extraTarget);
        return connection(id, "WEBHOOK", target, filter, "CANONICAL", null, secrets);
    }

    public static Map<String, Object> connection(long id, String type, Map<String, Object> target, Map<String, Object> filter,
                                                 String format, String template, Map<String, String> secrets) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("id", Long.toString(id));
        c.put("organizationId", Long.toString(ORG));
        c.put("name", "out-" + id);
        c.put("type", type);
        c.put("target", target);
        c.put("filter", filter == null ? Map.of() : filter);
        c.put("format", format);
        c.put("template", template);
        c.put("secrets", secrets);
        c.put("enabled", true);
        c.put("version", 1);
        return c;
    }

    public static OutputConnection parse(Map<String, Object> json) {
        JsonNode n = Json.MAPPER.valueToTree(json);
        return OutputConnection.parse(n);
    }
}
