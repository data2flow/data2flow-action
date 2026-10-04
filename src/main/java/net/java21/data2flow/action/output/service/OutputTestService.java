package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.domain.DeliveryResult;
import net.java21.data2flow.action.output.domain.DeviceContext;
import net.java21.data2flow.action.output.domain.HostGuard;
import net.java21.data2flow.action.output.domain.OutboundMessage;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.action.output.domain.OutputRenderer;
import net.java21.data2flow.action.output.transport.MqttOutputPublisher;
import net.java21.data2flow.action.output.transport.WebhookOutputSender;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.output.OutputConnectionType;
import net.java21.data2flow.contracts.output.OutputFailureKind;
import tools.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 출력 연결 테스트(API-DSC-76, core API-DSC-32가 중계). 샘플 텔레메트리를 필터·형식대로 만들어 대상에 <b>실제로 하나</b> 보내고 결과를
 * 돌려준다(필터가 샘플을 모두 거르면 샘플 그대로). 실패도 200 + {@code ok=false}·{@code failureKind}. 공용 브로커 주소는 보내지 않고
 * REFUSED다(CLAUDE.md §5).
 */
public class OutputTestService {

    private final MqttOutputPublisher mqtt;
    private final WebhookOutputSender webhook;
    private final List<String> deniedHosts;

    public OutputTestService(MqttOutputPublisher mqtt, WebhookOutputSender webhook, List<String> deniedHosts) {
        this.mqtt = mqtt;
        this.webhook = webhook;
        this.deniedHosts = List.copyOf(deniedHosts);
    }

    public Map<String, Object> test(JsonNode body) {
        OutputConnection c = OutputConnection.parse(body);
        CanonicalTelemetry sample = Json.MAPPER.treeToValue(body.path("sample"), CanonicalTelemetry.class);
        JsonNode ctxNode = body.path("context");
        DeviceContext ctx = ctxNode.isObject() ? DeviceContext.parse(ctxNode) : DeviceContext.unknown(sample.deviceId(), c.organizationId());
        List<OutboundMessage> messages = OutputRenderer.render(c, sample, ctx);
        if (messages.isEmpty()) {
            messages = OutputRenderer.renderSelected(c, sample, ctx);
        }
        boolean isMqtt = c.type() == OutputConnectionType.MQTT_PUBLISH;
        String rendered = isMqtt ? messages.getFirst().body() : WebhookOutputSender.batchBody(c, messages);
        DeliveryResult result;
        if (HostGuard.denied(c.url(), deniedHosts)) {
            result = DeliveryResult.failure(OutputFailureKind.REFUSED, "FORBIDDEN_HOST", null, null);
        } else if (isMqtt) {
            result = mqtt.test(c, messages.getFirst());
        } else {
            result = webhook.post(c, rendered);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", result.ok());
        out.put("failureKind", result.failureKind() == null ? null : result.failureKind().name());
        out.put("rendered", rendered);
        out.put("topic", isMqtt ? messages.getFirst().topic() : null);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", result.status());
        response.put("bodyPreview", result.ok() ? result.bodyPreview() : result.bodyPreview() == null ? result.error() : result.bodyPreview());
        out.put("response", response);
        return out;
    }
}
