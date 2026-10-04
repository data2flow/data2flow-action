package net.java21.data2flow.action.sink.service;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.contracts.secret.Secret;
import org.springframework.web.client.RestClient;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * core 내부 API-FLW-85 {@code GET /internal/core/sink-connections/{connection-id}}(ADR-021: 토큰 없음). core가 비밀값을 복호화해
 * {@code secrets}로 준다(소스 런타임 설정과 같은 방식). 응답은 로그에 남기지 않고 비밀값은 {@link Secret}으로만 감싼다.
 */
public class SinkConnectionClient {

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };
    private final RestClient core;

    public SinkConnectionClient(RestClient core) {
        this.core = core;
    }

    /** 연결 정의. 없으면 빈 값, core 장애는 예외(일시 실패) */
    public Optional<SinkConnection> find(long connectionId) {
        JsonNode body = core.get().uri("/internal/core/sink-connections/{id}", connectionId)
                .header(DataflowHeaders.CALLER_SERVICE, "data2flow-action")
                .exchange((req, res) -> {
                    int status = res.getStatusCode().value();
                    if (status == 404) {
                        return null;
                    }
                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("core 내부 API 실패: HTTP " + status);
                    }
                    return res.bodyTo(JsonNode.class);
                });
        if (body == null) {
            return Optional.empty();
        }
        return Optional.of(parse(body.path("response")));
    }

    static SinkConnection parse(JsonNode r) {
        Map<String, Object> config = r.path("config").isObject() ? Json.MAPPER.convertValue(r.path("config"), MAP) : Map.of();
        Map<String, Secret> secrets = new LinkedHashMap<>();
        r.path("secrets").properties().forEach(e -> {
            if (!e.getValue().isNull()) {
                secrets.put(e.getKey(), Secret.of(e.getValue().asString()));
            }
        });
        return new SinkConnection(r.path("connectionId").asLong(), r.path("organizationId").asLong(), r.path("type").asString(),
                config, secrets, r.path("version").asLong(0));
    }
}
