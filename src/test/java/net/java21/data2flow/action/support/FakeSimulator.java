package net.java21.data2flow.action.support;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.capability.CapabilityStates;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 가짜 시뮬레이터(API-SIM-30·32). 실제 시뮬레이터처럼 명령을 202로 받고, 반응 모드에 따라 EVT-SIM-03({@code device.command.ack}·
 * {@code device.state.reported})을 낸다. 같은 commandId는 한 번만 적용한다.
 */
public final class FakeSimulator extends Dispatcher {

    /** 기기 반응 */
    public enum Mode {
        /** ack + 상태 보고 */
        RESPOND,
        /** 202만(응답 실패 확률 100%) */
        SILENT,
        /** 400(처리 시점 상태로 적용 불가) */
        REJECT,
        /** 503(일시 장애) */
        ERROR
    }

    /** 이벤트 발행: (type, payload JSON 객체) */
    public interface Events {
        void ack(long deviceId, String commandId, boolean acked);

        void reported(long deviceId, long version, Map<String, Map<String, Object>> capabilities);
    }

    private static final Pattern COMMAND = Pattern.compile("/internal/sim/devices/(\\d+)/commands");
    private static final Pattern STATE = Pattern.compile("/internal/sim/devices/(\\d+)/state");

    public final MockWebServer server = new MockWebServer();
    public final Map<Long, Mode> modes = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, Map<String, Object>>> states = new ConcurrentHashMap<>();
    public final Map<Long, Long> versions = new ConcurrentHashMap<>();
    public final List<JsonNode> received = new CopyOnWriteArrayList<>();
    public final Set<String> applied = ConcurrentHashMap.newKeySet();
    public volatile Events events;

    public FakeSimulator() {
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
        modes.clear();
        states.clear();
        versions.clear();
        received.clear();
        applied.clear();
    }

    /** 기기의 현재 상태를 정한다(리모컨으로 바꾼 것처럼) */
    public void setState(long deviceId, Map<String, Map<String, Object>> state) {
        states.put(deviceId, state);
    }

    public long commandsFor(long deviceId) {
        return received.stream().filter(n -> n.path("deviceId").asLong() == deviceId).count();
    }

    @Override
    public MockResponse dispatch(RecordedRequest request) {
        String path = request.getPath() == null ? "" : request.getPath();
        Matcher m = COMMAND.matcher(path);
        if (m.matches()) {
            long deviceId = Long.parseLong(m.group(1));
            JsonNode body = Json.MAPPER.readTree(request.getBody().readUtf8());
            ((tools.jackson.databind.node.ObjectNode) body).put("deviceId", deviceId);
            received.add(body);
            Mode mode = modes.getOrDefault(deviceId, Mode.RESPOND);
            switch (mode) {
                case REJECT -> {
                    return new MockResponse().setResponseCode(400);
                }
                case ERROR -> {
                    return new MockResponse().setResponseCode(503);
                }
                case SILENT -> {
                    return accepted(body);
                }
                default -> {
                }
            }
            String commandId = body.path("commandId").asString();
            if (applied.add(commandId)) {
                Map<String, Object> args = Json.MAPPER.convertValue(body.path("args"), Map.class);
                Map<String, Map<String, Object>> next = CapabilityStates.merge(states.getOrDefault(deviceId, Map.of()),
                        body.path("capability").asString(), args);
                states.put(deviceId, next);
                long version = versions.merge(deviceId, 1L, Long::sum);
                Events e = events;
                if (e != null) {
                    e.ack(deviceId, commandId, true);
                    e.reported(deviceId, version, next);
                }
            }
            return accepted(body);
        }
        m = STATE.matcher(path);
        if (m.matches()) {
            long deviceId = Long.parseLong(m.group(1));
            return json(200, Map.of("header", Map.of("isSuccessful", true), "response", Map.of("deviceId", String.valueOf(deviceId),
                    "externalId", "aircon-" + deviceId, "capabilities", states.getOrDefault(deviceId, Map.of()),
                    "version", versions.getOrDefault(deviceId, 0L), "reportedAt", "2026-03-02T00:00:00Z", "virtual", true)));
        }
        if (path.startsWith("/internal/sim/catalog")) {
            return json(200, Map.of("header", Map.of("isSuccessful", true), "response", Map.of()));
        }
        return new MockResponse().setResponseCode(404);
    }

    private static MockResponse accepted(JsonNode body) {
        return json(202, Map.of("header", Map.of("isSuccessful", true), "response",
                Map.of("accepted", true, "commandId", body.path("commandId").asString())));
    }

    private static MockResponse json(int status, Object body) {
        return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(Json.write(body));
    }
}
