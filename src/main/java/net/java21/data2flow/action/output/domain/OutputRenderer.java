package net.java21.data2flow.action.output.domain;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.output.OutputConnectionType;
import net.java21.data2flow.contracts.output.OutputFormat;
import net.java21.data2flow.contracts.output.OutputTopicTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 출력 연결 하나가 텔레메트리 하나로 보낼 메시지를 만든다(DSC-04.01, ADR-048 계약).
 * <ul>
 *   <li>필터: contracts {@code OutputFilter.select}(기기·그룹·공간 중 하나, 측정 항목·최소 품질은 측정값마다). 남는 것이 없으면 보내지 않는다</li>
 *   <li>CANONICAL: MQTT는 텔레메트리 하나에 메시지 하나(토픽에 {@code {metric}}이 있으면 측정값마다, 본문은 그 측정값만 남긴 표준 메시지),
 *       Webhook은 걸러진 표준 메시지 하나가 배치 항목 하나</li>
 *   <li>TEMPLATE: 측정값마다 로직 없는 템플릿({@link TemplateLite})을 채운 본문 하나</li>
 * </ul>
 */
public final class OutputRenderer {

    private OutputRenderer() {
    }

    /** 걸러서 만든다. 대상이 아니면 빈 목록 */
    public static List<OutboundMessage> render(OutputConnection c, CanonicalTelemetry t, DeviceContext ctx) {
        Optional<CanonicalTelemetry> selected = c.filter().select(t, ctx.groupIds(), ctx.spacePathIds());
        return selected.map(s -> renderSelected(c, s, ctx)).orElse(List.of());
    }

    /** 이미 거른 텔레메트리로 만든다(연결 테스트는 필터가 모두 걸러도 샘플을 그대로 보낸다) */
    public static List<OutboundMessage> renderSelected(OutputConnection c, CanonicalTelemetry t, DeviceContext ctx) {
        boolean mqtt = c.type() == OutputConnectionType.MQTT_PUBLISH;
        OutputTopicTemplate topic = mqtt ? c.topicTemplate() : null;
        List<OutboundMessage> out = new ArrayList<>();
        if (c.format() == OutputFormat.TEMPLATE) {
            String template = c.template() == null ? "" : c.template();
            for (CanonicalTelemetry.Metric m : t.metrics()) {
                out.add(new OutboundMessage(m.key(), mqtt ? topic.render(topicValues(ctx, t, m.key())) : null,
                        TemplateLite.render(template, templateValues(ctx, t, m))));
            }
            return out;
        }
        if (mqtt && topic.perMetric()) {
            for (CanonicalTelemetry.Metric m : t.metrics()) {
                out.add(new OutboundMessage(m.key(), topic.render(topicValues(ctx, t, m.key())), Json.write(t.withMetrics(List.of(m)))));
            }
            return out;
        }
        out.add(new OutboundMessage("", mqtt ? topic.render(topicValues(ctx, t, null)) : null, Json.write(t)));
        return out;
    }

    static Map<String, String> topicValues(DeviceContext ctx, CanonicalTelemetry t, String metric) {
        Map<String, String> v = new HashMap<>();
        v.put(OutputTopicTemplate.DEVICE_ID, Long.toString(t.deviceId()));
        v.put(OutputTopicTemplate.DEVICE_NAME, ctx.deviceName() == null ? t.externalId() : ctx.deviceName());
        v.put(OutputTopicTemplate.SPACE_CODE, ctx.spaceCode());
        v.put(OutputTopicTemplate.METRIC, metric);
        return v;
    }

    static Map<String, String> templateValues(DeviceContext ctx, CanonicalTelemetry t, CanonicalTelemetry.Metric m) {
        Map<String, String> v = new LinkedHashMap<>();
        v.put("deviceId", Long.toString(t.deviceId()));
        v.put("deviceName", ctx.deviceName() == null ? t.externalId() : ctx.deviceName());
        v.put("spaceCode", ctx.spaceCode());
        v.put("organizationId", Long.toString(t.organizationId()));
        v.put("metric", m.key());
        v.put("value", number(m.value()));
        v.put("unit", m.unit());
        v.put("quality", Integer.toString(m.quality()));
        v.put("measuredAt", t.measuredAt().toString());
        return v;
    }

    static String number(Double value) {
        if (value == null) {
            return "";
        }
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
