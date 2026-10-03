package net.java21.data2flow.action.support;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.authz.BuiltinRole;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 가짜 core-api 내부 API(MockWebServer, testing/backend.md "외부 HTTP 대역"): 제어 프로필(API-ACT-40), 샌드박스 공간(API-ACT-41),
 * 관계 대상(API-DEV-128), 권한 판정, 감사 기록(API-IAM-39).
 */
public final class FakeCore extends Dispatcher {

    private static final Pattern PROFILE = Pattern.compile("/internal/core/devices/(\\d+)/control-profile");
    private static final Pattern GRANT = Pattern.compile("/internal/core/organizations/(\\d+)/users/(\\d+)/access-grant");
    private static final Pattern SPACE_DEVICES = Pattern.compile("/internal/core/spaces/(\\d+)/devices.*");

    public final MockWebServer server = new MockWebServer();
    public final Map<Long, ControlProfile> profiles = new ConcurrentHashMap<>();
    public final Set<Long> sandbox = ConcurrentHashMap.newKeySet();
    public final Map<Long, String> roles = new ConcurrentHashMap<>();
    public final Map<Long, List<Long>> spaceDevices = new ConcurrentHashMap<>();
    public final List<JsonNode> audits = new CopyOnWriteArrayList<>();
    public final AtomicInteger profileCalls = new AtomicInteger();
    public final AtomicInteger sandboxCalls = new AtomicInteger();
    public volatile boolean auditDown;

    public FakeCore() {
        server.setDispatcher(this);
        try {
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public String url() {
        return "http://localhost:" + server.getPort();
    }

    public void reset() {
        profiles.clear();
        sandbox.clear();
        roles.clear();
        spaceDevices.clear();
        audits.clear();
        profileCalls.set(0);
        sandboxCalls.set(0);
        auditDown = false;
    }

    @Override
    public MockResponse dispatch(RecordedRequest request) {
        String path = request.getPath() == null ? "" : request.getPath();
        Matcher m = PROFILE.matcher(path);
        if (m.matches()) {
            profileCalls.incrementAndGet();
            ControlProfile p = profiles.get(Long.parseLong(m.group(1)));
            return p == null ? json(404, Map.of("header", Map.of("isSuccessful", false, "resultCode", "DEVICE_NOT_FOUND")))
                    : ok(p);
        }
        if (path.startsWith("/internal/core/sim/sandbox-spaces")) {
            sandboxCalls.incrementAndGet();
            return ok(Map.of("spaceIds", sandbox.stream().map(String::valueOf).toList()));
        }
        m = GRANT.matcher(path);
        if (m.matches()) {
            String role = roles.get(Long.parseLong(m.group(2)));
            if (role == null) {
                return ok(Map.of("active", false));
            }
            List<String> permissions = BuiltinRole.valueOf(role).permissions().stream().map(Enum::name).toList();
            return ok(Map.of("userId", m.group(2), "organizationId", m.group(1), "active", true, "role", role, "permissions", permissions,
                    "spaceScope", Map.of("unrestricted", true, "allowedSpaceIds", List.of())));
        }
        m = SPACE_DEVICES.matcher(path);
        if (m.matches()) {
            List<Long> ids = spaceDevices.getOrDefault(Long.parseLong(m.group(1)), List.of());
            return ok(ids.stream().map(id -> Map.of("deviceId", String.valueOf(id), "relation", "CONTROLS")).toList());
        }
        if (path.startsWith("/internal/core/audit-logs")) {
            if (auditDown) {
                return new MockResponse().setResponseCode(503);
            }
            audits.add(Json.MAPPER.readTree(request.getBody().readUtf8()));
            return new MockResponse().setResponseCode(202);
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse ok(Object response) {
        return json(200, Map.of("header", Map.of("isSuccessful", true, "resultCode", "SUCCESS", "resultMessage", "성공"),
                "response", response));
    }

    private static MockResponse json(int status, Object body) {
        return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(Json.write(body));
    }
}
