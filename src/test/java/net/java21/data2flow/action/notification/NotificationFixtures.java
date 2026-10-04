package net.java21.data2flow.action.notification;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.FakeTelegram;
import okhttp3.mockwebserver.MockResponse;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 알림 시험 데이터(RUL test-plan {@code RuleFixtures}, OPS {@code TestOrganizations}·{@code UserFixtures})와 가짜 core 알림 내부 API
 * (API-OPS-35, API-RUL-40~50). {@link #install(FakeCore)}로 {@code FakeCore.routes}에 건다.
 */
public final class NotificationFixtures {

    public static final long ORG = 1;
    public static final long TELEGRAM_CHANNEL = 4;
    public static final long FAKE_CHANNEL = 6;
    public static final String CHAT = "-100777";
    public static final long ALARM = 9001;
    public static final long SPACE = 31;
    public static final long RULE = 12;

    public final Map<Long, Map<String, Object>> channels = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, Object>> alarms = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, Object>> users = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, Object>> policies = new ConcurrentHashMap<>();
    public final List<Map<String, Object>> silences = new CopyOnWriteArrayList<>();
    public final Map<String, Long> links = new ConcurrentHashMap<>();
    public final Map<String, Long> linkCodes = new ConcurrentHashMap<>();
    public final List<String> alarmActions = new CopyOnWriteArrayList<>();
    public final List<JsonNode> alarmEvents = new CopyOnWriteArrayList<>();
    public volatile Map<String, Object> onCall = Map.of("timezone", "Asia/Seoul", "shifts", List.of(), "overrides", List.of());
    public volatile Map<String, Object> template;

    public NotificationFixtures() {
        channels.put(TELEGRAM_CHANNEL, telegramChannel(TELEGRAM_CHANNEL, 20, 60));
    }

    public static Map<String, Object> telegramChannel(long id, int ratePerMin, int digestSec) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("channelId", String.valueOf(id));
        c.put("organizationId", String.valueOf(ORG));
        c.put("name", "시설팀 텔레그램");
        c.put("type", "TELEGRAM");
        c.put("config", Map.of("chatIds", List.of(CHAT), "botUsername", "data2flow_test_bot"));
        c.put("secrets", Map.of("botToken", FakeTelegram.TOKEN, "webhookSecret", FakeTelegram.SECRET));
        c.put("rateLimitPerMin", ratePerMin);
        c.put("digestWindowSec", digestSec);
        c.put("enabled", true);
        return c;
    }

    public static Map<String, Object> fakeChannel(long id) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("channelId", String.valueOf(id));
        c.put("organizationId", String.valueOf(ORG));
        c.put("name", "가짜 채널");
        c.put("type", "FAKE");
        c.put("config", Map.of("room", "ops"));
        c.put("secrets", Map.of("secret", "fake-secret"));
        c.put("rateLimitPerMin", 60);
        c.put("digestWindowSec", 60);
        c.put("enabled", true);
        return c;
    }

    /** 실습실(공간 31) 고온 알람 */
    public static Map<String, Object> alarm(long id, String severity, String status) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("id", id);
        a.put("alarmKey", "rule:" + RULE + ":15:temperature");
        a.put("severity", severity);
        a.put("status", status);
        a.put("flapping", false);
        a.put("title", "실습실 고온");
        a.put("source", Map.of("type", "RULE", "ruleId", RULE));
        a.put("device", Map.of("id", 15, "name", "실습실 온도계"));
        a.put("space", Map.of("id", SPACE, "path", "본관/3층/실습실", "pathIds", List.of(1, 7, SPACE)));
        a.put("metric", "temperature");
        a.put("lastValue", 27.6);
        a.put("occurrenceCount", 1);
        a.put("raisedAt", "2026-03-02T00:00:00Z");
        return a;
    }

    /** 사용자(텔레그램 연결 = externalUserId) */
    public static Map<String, Object> user(long id, String role, String telegramId) {
        Map<String, Object> u = new LinkedHashMap<>();
        u.put("userId", String.valueOf(id));
        u.put("role", role);
        u.put("active", true);
        u.put("locale", "ko");
        u.put("timezone", "Asia/Seoul");
        u.put("links", telegramId == null ? Map.of() : Map.of("TELEGRAM", telegramId));
        u.put("spaceScope", Map.of("unrestricted", true, "allowedSpaceIds", List.of()));
        return u;
    }

    public void install(FakeCore core) {
        core.routes.put("/internal/core/notification-channels", r -> {
            String path = r.getPath();
            String tail = path.substring("/internal/core/notification-channels".length());
            if (tail.startsWith("/")) {
                Map<String, Object> c = channels.get(Long.parseLong(tail.substring(1).split("\\?")[0]));
                return c == null ? FakeCore.json(404, Map.of()) : FakeCore.ok(c);
            }
            String type = query(path, "type");
            List<Map<String, Object>> list = channels.values().stream().filter(c -> type == null || type.equals(c.get("type"))).toList();
            return FakeCore.json(200, (Map.of("header", Map.of("isSuccessful", true), "responses", list,
                    "totalCount", list.size())));
        });
        core.routes.put("/internal/core/alarms/", r -> {
            String[] parts = r.getPath().split("\\?")[0].split("/");
            long id = Long.parseLong(parts[4]);
            if ("POST".equals(r.getMethod())) {
                String action = parts[5];
                if ("events".equals(action)) {
                    alarmEvents.add(Json.MAPPER.readTree(r.getBody().readUtf8()));
                    return new MockResponse().setResponseCode(202);
                }
                alarmActions.add(action + ":" + id + ":user=" + r.getHeader("X-USER-ID"));
                return FakeCore.ok(Map.of("ok", true));
            }
            Map<String, Object> a = alarms.get(id);
            return a == null ? FakeCore.json(404, Map.of()) : FakeCore.ok(a);
        });
        core.routes.put("/internal/core/organizations/" + ORG + "/notify-recipients", r -> {
            String ids = query(r.getPath(), "userIds");
            String roles = query(r.getPath(), "roles");
            List<String> idList = ids == null ? List.of() : List.of(ids.split(","));
            List<String> roleList = roles == null ? List.of() : List.of(roles.split(","));
            List<Map<String, Object>> out = new ArrayList<>();
            for (Map<String, Object> u : users.values()) {
                if (idList.contains(u.get("userId")) || roleList.contains(u.get("role"))) {
                    out.add(u);
                }
            }
            return FakeCore.json(200, (Map.of("header", Map.of("isSuccessful", true), "responses", out,
                    "totalCount", out.size())));
        });
        core.routes.put("/internal/core/silences", r -> FakeCore.json(200, (Map.of("header", Map.of("isSuccessful", true),
                "responses", silences, "totalCount", silences.size()))));
        core.routes.put("/internal/core/on-call", r -> FakeCore.ok(onCall));
        core.routes.put("/internal/core/notification-templates/resolve", r -> template == null ? FakeCore.json(404, Map.of())
                : FakeCore.ok(template));
        core.routes.put("/internal/core/notification-policies/", r -> {
            long id = Long.parseLong(r.getPath().split("\\?")[0].substring("/internal/core/notification-policies/".length()));
            Map<String, Object> p = policies.get(id);
            return p == null ? FakeCore.json(404, Map.of()) : FakeCore.ok(p);
        });
        core.routes.put("/internal/core/messenger-links/confirm", r -> {
            JsonNode body = Json.MAPPER.readTree(r.getBody().readUtf8());
            Long user = linkCodes.remove(body.path("code").asString());
            if (user == null) {
                return FakeCore.json(404, Map.of());
            }
            links.put(body.path("externalUserId").asString(), user);
            return FakeCore.ok(Map.of("userId", String.valueOf(user), "organizationId", String.valueOf(ORG)));
        });
        core.routes.put("/internal/core/messenger-links", r -> {
            Long user = links.get(query(r.getPath(), "externalUserId"));
            return user == null ? FakeCore.json(404, Map.of())
                    : FakeCore.ok(Map.of("userId", String.valueOf(user), "organizationId", String.valueOf(ORG)));
        });
    }

    static String query(String path, String name) {
        int q = path.indexOf('?');
        if (q < 0) {
            return null;
        }
        for (String kv : path.substring(q + 1).split("&")) {
            String[] p = kv.split("=", 2);
            if (p[0].equals(name)) {
                return p.length < 2 ? "" : java.net.URLDecoder.decode(p[1], java.nio.charset.StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
