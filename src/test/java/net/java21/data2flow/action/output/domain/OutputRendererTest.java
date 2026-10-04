package net.java21.data2flow.action.output.domain;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.OutputFixtures;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 출력 메시지 만들기(DSC-04.01): 필터·토픽 템플릿·형식 */
class OutputRendererTest {

    private static final Instant T = Instant.parse("2026-10-04T01:00:00Z");

    @Test
    @DisplayName("[DSC-04.01][AT-DSC-10.1][TC-DSC-126] 필터 metrics=[co2]면 실습실 AM107 수신에서 co2만 전달(측정값마다 토픽 d2f/{spaceCode}/{deviceName}/{metric})")
    void co2Only() {
        OutputConnection c = OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://broker.example:1883", "d2f/{spaceCode}/{deviceName}/{metric}",
                Map.of("metrics", List.of("co2"))));
        List<OutboundMessage> out = OutputRenderer.render(c, OutputFixtures.am107(T, 812), OutputFixtures.context());
        assertThat(out).hasSize(1);
        assertThat(out.getFirst().topic()).isEqualTo("d2f/R101/am107-lab/co2");
        assertThat(out.getFirst().part()).isEqualTo("co2");
        CanonicalTelemetry sent = Json.read(out.getFirst().body(), CanonicalTelemetry.class);
        assertThat(sent.metrics()).extracting(CanonicalTelemetry.Metric::key).containsExactly("co2");
        assertThat(sent.metric("co2").value()).isEqualTo(812.0);
    }

    @Test
    @DisplayName("[DSC-04.01][TC-DSC-126] 기기·그룹·공간 조건 중 하나만 맞아도 대상, 모두 안 맞으면 보내지 않음, 최소 품질로 거름")
    void targetConditions() {
        CanonicalTelemetry t = OutputFixtures.am107(T, 700);
        DeviceContext ctx = OutputFixtures.context();
        String tpl = "d2f/{deviceId}";
        assertThat(OutputRenderer.render(OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://b", tpl, Map.of("groupIds", List.of("7")))), t, ctx))
                .hasSize(1);
        assertThat(OutputRenderer.render(OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://b", tpl, Map.of("spaceIds", List.of("30")))), t, ctx))
                .as("상위 공간이면 하위 공간 기기도 대상").hasSize(1);
        assertThat(OutputRenderer.render(OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://b", tpl,
                Map.of("deviceIds", List.of("9"), "spaceIds", List.of("99")))), t, ctx)).isEmpty();
        CanonicalTelemetry suspect = t.withMetrics(List.of(t.metric("co2").withQuality(3)));
        assertThat(OutputRenderer.render(OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://b", tpl, Map.of("qualityMin", 0))), suspect, ctx))
                .as("의심 값(3)은 qualityMin=0이면 거름").isEmpty();
    }

    @Test
    @DisplayName("[DSC-04.01] CANONICAL: 토픽에 {metric}이 없으면 텔레메트리 하나에 메시지 하나(빈 변수는 _), Webhook은 표준 메시지 그대로")
    void canonicalWhole() {
        CanonicalTelemetry t = OutputFixtures.am107(T, 700);
        DeviceContext noSpace = new DeviceContext(OutputFixtures.DEVICE, 1, null, null, null, List.of(), java.util.Set.of());
        List<OutboundMessage> mqtt = OutputRenderer.render(OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://b",
                "site/{spaceCode}/{deviceName}", Map.of())), t, noSpace);
        assertThat(mqtt).singleElement().satisfies(m -> {
            assertThat(m.topic()).isEqualTo("site/_/24e124710c408089");
            assertThat(m.part()).isEmpty();
            assertThat(Json.read(m.body(), CanonicalTelemetry.class).metrics()).hasSize(3);
        });
        List<OutboundMessage> hook = OutputRenderer.render(OutputFixtures.parse(OutputFixtures.webhook(2, "https://h.example/in", Map.of(),
                Map.of(), Map.of())), t, noSpace);
        assertThat(hook).singleElement().satisfies(m -> assertThat(m.topic()).isNull());
    }

    @Test
    @DisplayName("[DSC-04.01] TEMPLATE: 측정값마다 로직 없는 템플릿({{deviceName}} {{metric}} {{value}} {{unit}} {{measuredAt}}), 모르는 변수는 빈 값")
    void template() {
        Map<String, Object> json = OutputFixtures.connection(3, "WEBHOOK", Map.of("url", "https://h.example/in"), Map.of("metrics", List.of("co2",
                "temperature")), "TEMPLATE", "{\"d\":\"{{deviceName}}\",\"m\":\"{{metric}}\",\"v\":{{{value}}},\"u\":\"{{unit}}\",\"at\":\"{{measuredAt}}\",\"x\":\"{{nope}}\"}",
                Map.of());
        List<OutboundMessage> out = OutputRenderer.render(OutputFixtures.parse(json), OutputFixtures.am107(T, 812), OutputFixtures.context());
        assertThat(out).hasSize(2);
        JsonNode co2 = Json.MAPPER.readTree(out.get(1).body());
        assertThat(out.get(0).part()).isEqualTo("temperature");
        assertThat(co2.path("d").asString()).isEqualTo("am107-lab");
        assertThat(co2.path("v").asDouble()).isEqualTo(812.0);
        assertThat(co2.path("u").asString()).isEqualTo("ppm");
        assertThat(co2.path("at").asString()).isEqualTo("2026-10-04T01:00:00Z");
        assertThat(co2.path("x").asString()).isEmpty();
        assertThat(OutputRenderer.number(23.50)).isEqualTo("23.5");
        assertThat(OutputRenderer.number(null)).isEmpty();
    }

    @Test
    @DisplayName("[DSC-04.01][TC-DSC-130] 허용 밖 토픽 변수는 만들 수 없다(core 저장 검사와 같은 규칙)")
    void unknownVariable() {
        OutputConnection c = OutputFixtures.parse(OutputFixtures.mqtt(1, "mqtt://b", "d2f/{tenant}/{metric}", Map.of()));
        assertThatThrownBy(() -> OutputRenderer.render(c, OutputFixtures.am107(T, 1), OutputFixtures.context()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("[DSC-04.01] 정의 읽기: ID는 문자열·숫자 모두, 형식 기본 CANONICAL, 모르는 종류는 UNKNOWN, 비밀값은 toString에 나오지 않음")
    void parse() {
        Map<String, Object> json = new java.util.LinkedHashMap<>(OutputFixtures.webhook(4, "https://h", null, Map.of(), Map.of("HMAC_KEY", "s3cr3t")));
        json.put("id", 4);
        json.put("format", null);
        OutputConnection c = OutputFixtures.parse(json);
        assertThat(c.id()).isEqualTo(4);
        assertThat(c.format()).isEqualTo(net.java21.data2flow.contracts.output.OutputFormat.CANONICAL);
        assertThat(c.toString()).doesNotContain("s3cr3t").contains("HMAC_KEY");
        json.put("type", "CARRIER_PIGEON");
        json.put("format", "XML");
        assertThat(OutputFixtures.parse(json).type()).isEqualTo(net.java21.data2flow.contracts.output.OutputConnectionType.UNKNOWN);
        assertThat(OutputFixtures.parse(json).format()).isEqualTo(net.java21.data2flow.contracts.output.OutputFormat.UNKNOWN);
        DeviceContext ctx = DeviceContext.parse(Json.MAPPER.valueToTree(Map.of("deviceId", 5, "organizationId", "1", "spacePathIds", List.of(1, "2"))));
        assertThat(ctx.spaceId()).isNull();
        assertThat(ctx.spacePathIds()).containsExactly(1L, 2L);
    }
}
