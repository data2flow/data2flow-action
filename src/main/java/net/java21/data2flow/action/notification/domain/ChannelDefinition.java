package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.notification.ChannelSettings;
import net.java21.data2flow.contracts.secret.Secret;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 알림 채널 한 개(core {@code notification_channels}, API-OPS-35). 비밀값은 core가 복호화해 주고 여기서는 {@link Secret}으로만 든다
 * ({@code toString}·로그에 평문이 나가지 않음).
 */
public record ChannelDefinition(long channelId, long organizationId, String name, String type, JsonNode config,
                                Map<String, Secret> secrets, int rateLimitPerMin, int digestWindowSec, boolean enabled) {

    public ChannelDefinition {
        secrets = secrets == null ? Map.of() : Map.copyOf(secrets);
    }

    public static ChannelDefinition from(JsonNode r) {
        Map<String, Secret> secrets = new LinkedHashMap<>();
        r.path("secrets").properties().forEach(e -> {
            if (e.getValue().isString() && !e.getValue().asString().isEmpty()) {
                secrets.put(e.getKey(), Secret.of(e.getValue().asString()));
            }
        });
        return new ChannelDefinition(r.path("channelId").asLong(r.path("id").asLong()), r.path("organizationId").asLong(),
                r.path("name").asString(""), r.path("type").asString(""), r.path("config"), secrets,
                r.path("rateLimitPerMin").asInt(20), r.path("digestWindowSec").asInt(60), r.path("enabled").asBoolean(true));
    }

    public ChannelSettings settings() {
        return new ChannelSettings(channelId, organizationId, config == null || config.isMissingNode() ? empty() : config, secrets);
    }

    /** 채널 기본 대화방(텔레그램 {@code chatIds} 또는 {@code defaultChatIds}) */
    public List<String> defaultAddresses() {
        List<String> out = new ArrayList<>();
        for (String field : List.of("chatIds", "defaultChatIds", "defaultChatId", "room")) {
            JsonNode n = config == null ? null : config.path(field);
            if (n == null || n.isMissingNode() || n.isNull()) {
                continue;
            }
            if (n.isArray()) {
                n.forEach(v -> out.add(v.asString()));
            } else {
                out.add(n.asString());
            }
        }
        return out;
    }

    private static JsonNode empty() {
        return net.java21.data2flow.action.common.Json.MAPPER.createObjectNode();
    }
}
