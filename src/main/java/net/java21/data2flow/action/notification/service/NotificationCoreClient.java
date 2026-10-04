package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.domain.AlarmInfo;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.OnCallSchedule;
import net.java21.data2flow.action.notification.domain.PolicyDefinition;
import net.java21.data2flow.action.notification.domain.RecipientProfile;
import net.java21.data2flow.action.notification.domain.Silence;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 알림 공통 계층이 부르는 core 내부 API(ADR-021: 토큰 없음, {@code X-CALLER-SERVICE: data2flow-action}). 정의(채널·정책·무음·당직·
 * 템플릿·수신 설정)는 {@code cacheTtl}까지 캐시하고 {@code data2flow.config} 변경 메시지로 지운다. 알람 상태는 캐시하지 않는다.
 *
 * <ul>
 *   <li>API-OPS-35 채널(비밀값은 core가 복호화해 줌 — 로그에 남기지 않는다)</li>
 *   <li>API-RUL-40 정책 · 41 알람 · 42 수신자 · 43 무음 · 44 당직 · 45 템플릿 · 46 메신저 연결 조회 · 47 연결 확정 · 48 확인 · 49 무음</li>
 * </ul>
 */
public class NotificationCoreClient {

    private static final String CALLER = "data2flow-action";
    private final RestClient core;
    private final Clock clock;
    private final Duration ttl;
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    public NotificationCoreClient(RestClient core, Clock clock, Duration ttl) {
        this.core = core;
        this.clock = clock;
        this.ttl = ttl;
    }

    private record Cached(Object value, Instant expiresAt) {
    }

    @SuppressWarnings("unchecked")
    private <T> T cached(String key, Supplier<T> load) {
        Instant now = clock.instant();
        Cached c = cache.get(key);
        if (c != null && c.expiresAt().isAfter(now)) {
            return (T) c.value();
        }
        T value = load.get();
        cache.put(key, new Cached(value, now.plus(ttl)));
        return value;
    }

    /** 설정 변경·재연결: 모두 다시 읽게 한다 */
    public void invalidateAll() {
        cache.clear();
    }

    // ───────────── 조회 ─────────────

    public Optional<AlarmInfo> alarm(long alarmId) {
        JsonNode body = get("/internal/core/alarms/{id}", alarmId);
        return body == null ? Optional.empty() : Optional.of(AlarmInfo.from(body.path("response")));
    }

    public Optional<PolicyDefinition> policy(long policyId) {
        return cached("policy:" + policyId, () -> {
            JsonNode body = get("/internal/core/notification-policies/{id}", policyId);
            return body == null ? Optional.empty() : Optional.of(PolicyDefinition.from(body.path("response")));
        });
    }

    /** 조직(없으면 배포 전체)의 켜진 채널 중 이 종류 */
    public List<ChannelDefinition> channels(Long organizationId, String type) {
        return cached("channels:" + organizationId + ":" + type, () -> {
            JsonNode body = core.get().uri(b -> b.path("/internal/core/notification-channels")
                            .queryParamIfPresent("organizationId", Optional.ofNullable(organizationId))
                            .queryParam("type", type).build())
                    .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                    .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
            List<ChannelDefinition> out = new ArrayList<>();
            if (body != null) {
                body.path("responses").forEach(n -> out.add(ChannelDefinition.from(n)));
            }
            return out.stream().filter(ChannelDefinition::enabled).toList();
        });
    }

    public Optional<ChannelDefinition> channel(long channelId) {
        return cached("channel:" + channelId, () -> {
            JsonNode body = get("/internal/core/notification-channels/{id}", channelId);
            return body == null ? Optional.empty() : Optional.of(ChannelDefinition.from(body.path("response")));
        });
    }

    /** 사용자 ID 목록과 역할을 펼친 수신자(비활성 포함, 호출 쪽이 거른다) */
    public List<RecipientProfile> recipients(long organizationId, Collection<Long> userIds, Collection<String> roles) {
        if (userIds.isEmpty() && roles.isEmpty()) {
            return List.of();
        }
        String users = String.join(",", userIds.stream().sorted().map(String::valueOf).toList());
        String roleList = String.join(",", roles.stream().sorted().toList());
        return cached("recipients:" + organizationId + ":" + users + ":" + roleList, () -> {
            JsonNode body = core.get().uri(b -> {
                        b.path("/internal/core/organizations/{org}/notify-recipients");
                        if (!users.isEmpty()) {
                            b.queryParam("userIds", users);
                        }
                        if (!roleList.isEmpty()) {
                            b.queryParam("roles", roleList);
                        }
                        return b.build(organizationId);
                    })
                    .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                    .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
            List<RecipientProfile> out = new ArrayList<>();
            if (body != null) {
                body.path("responses").forEach(n -> out.add(RecipientProfile.from(n)));
            }
            return List.copyOf(out);
        });
    }

    public List<Silence> silences(long organizationId) {
        return cached("silences:" + organizationId, () -> {
            JsonNode body = core.get().uri(b -> b.path("/internal/core/silences").queryParam("organizationId", organizationId)
                            .queryParam("active", true).build())
                    .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                    .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
            List<Silence> out = new ArrayList<>();
            if (body != null) {
                body.path("responses").forEach(n -> out.add(Silence.from(n)));
            }
            return List.copyOf(out);
        });
    }

    public OnCallSchedule onCall(long organizationId) {
        return cached("oncall:" + organizationId, () -> {
            JsonNode body = core.get().uri(b -> b.path("/internal/core/on-call").queryParam("organizationId", organizationId).build())
                    .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                    .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
            return body == null ? OnCallSchedule.EMPTY : OnCallSchedule.from(body.path("response"));
        });
    }

    /** 조직 템플릿 {제목, 본문}. 없거나 core가 실패하면 빈 값(내장 기본 템플릿을 쓴다) */
    public Optional<String[]> template(long organizationId, String key, String channel, String locale) {
        return cached("template:" + organizationId + ":" + key + ":" + channel + ":" + locale, () -> {
            try {
                JsonNode body = core.get().uri(b -> b.path("/internal/core/notification-templates/resolve")
                                .queryParam("organizationId", organizationId).queryParam("key", key).queryParam("channel", channel)
                                .queryParam("locale", locale).build())
                        .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                        .exchange((req, res) -> res.getStatusCode().value() >= 300 ? null : res.bodyTo(JsonNode.class));
                if (body == null || !body.path("response").path("body").isString()) {
                    return Optional.<String[]>empty();
                }
                JsonNode r = body.path("response");
                return Optional.of(new String[]{r.path("subject").isString() ? r.path("subject").asString() : null, r.path("body").asString()});
            } catch (RuntimeException e) {
                return Optional.<String[]>empty();
            }
        });
    }

    // ───────────── 메신저 ─────────────

    /** 연결된 사용자 */
    public record LinkedUser(long userId, long organizationId) {
    }

    public Optional<LinkedUser> linkedUser(String channel, String externalUserId) {
        JsonNode body = core.get().uri(b -> b.path("/internal/core/messenger-links").queryParam("channel", channel)
                        .queryParam("externalUserId", externalUserId).build())
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
        return body == null ? Optional.empty() : Optional.of(linked(body.path("response")));
    }

    /** 연결 코드 확정(API-RUL-47). 코드가 없거나 만료면 빈 값 */
    public Optional<LinkedUser> confirmLink(String channel, String code, String externalUserId) {
        JsonNode body = core.post().uri("/internal/core/messenger-links/confirm")
                .header(DataflowHeaders.CALLER_SERVICE, CALLER).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("channel", channel, "code", code, "externalUserId", externalUserId))
                .exchange((req, res) -> {
                    int s = res.getStatusCode().value();
                    return s == 404 || s == 409 || s == 400 ? null : read(s, res.bodyTo(JsonNode.class));
                });
        return body == null ? Optional.empty() : Optional.of(linked(body.path("response")));
    }

    /** 연결된 사용자 권한으로 알람 확인(API-RUL-48) 또는 무음(API-RUL-49). HTTP 상태 */
    public int alarmAction(long alarmId, String action, LinkedUser user, Integer minutes) {
        Map<String, Object> body = minutes == null ? Map.of("via", "MESSENGER") : Map.of("via", "MESSENGER", "minutes", minutes);
        Integer status = core.post().uri("/internal/core/alarms/{id}/" + action, alarmId)
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .header(DataflowHeaders.USER_ID, Long.toString(user.userId()))
                .header(DataflowHeaders.ORG_ID, Long.toString(user.organizationId()))
                .contentType(MediaType.APPLICATION_JSON).body(body)
                .exchange((req, res) -> res.getStatusCode().value());
        return status == null ? 500 : status;
    }

    private static LinkedUser linked(JsonNode r) {
        return new LinkedUser(r.path("userId").asLong(), r.path("organizationId").asLong());
    }

    private JsonNode get(String path, Object... vars) {
        return core.get().uri(path, vars)
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
    }

    private static JsonNode read(int status, JsonNode body) {
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("core 내부 API 실패: HTTP " + status);
        }
        return body == null ? Json.MAPPER.createObjectNode() : body;
    }
}
