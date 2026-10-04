package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.common.Json;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * 클라우드 벤더 API 대역(MockWebServer, ADR-040 "공개 문서 예시 응답을 쓴 계약 테스트"). 실제 벤더 API는 부르지 않는다. 명령 요청을 기록하고
 * 요청 ID 헤더로 명령별 적용 횟수를 센다. 상태 조회 응답은 시험이 정한 본문이다.
 */
final class VendorApiFake extends Dispatcher {

    final MockWebServer server = new MockWebServer();
    final List<RecordedCall> calls = new CopyOnWriteArrayList<>();
    final Map<String, AtomicInteger> perRequestId = new ConcurrentHashMap<>();
    final String validToken;
    final String requestIdHeader;
    final Function<String, String> requestIdDecoder;
    volatile String mode = "OK";          // OK, SILENT(504), ERROR(503), RATE_LIMITED(429)
    volatile String accessToken;           // 현재 유효한 토큰(갱신 시험)
    volatile String refreshedToken;
    volatile String stateBody = "{}";
    volatile String commandResponse = "{}";

    record RecordedCall(String method, String path, JsonNode body) {
    }

    VendorApiFake(String validToken, String requestIdHeader, Function<String, String> requestIdDecoder) {
        this.validToken = validToken;
        this.accessToken = validToken;
        this.requestIdHeader = requestIdHeader;
        this.requestIdDecoder = requestIdDecoder;
        server.setDispatcher(this);
        try {
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    String url() {
        return "http://localhost:" + server.getPort();
    }

    @Override
    public MockResponse dispatch(RecordedRequest r) {
        String path = r.getPath() == null ? "" : r.getPath();
        if (path.equals("/oauth/token")) {
            String form = r.getBody().readUtf8();
            if (form.contains("refresh_token=rt-1") && refreshedToken != null) {
                accessToken = refreshedToken;
                return json(200, "{\"access_token\":\"" + refreshedToken + "\",\"expires_in\":3600}");
            }
            return json(400, "{\"error\":\"invalid_grant\"}");
        }
        if (!("Bearer " + accessToken).equals(r.getHeader("Authorization"))) {
            return json(401, "{\"error\":\"unauthorized\"}");
        }
        String bodyText = r.getBody().readUtf8();
        JsonNode body = bodyText.isBlank() ? null : Json.MAPPER.readTree(bodyText);
        calls.add(new RecordedCall(r.getMethod(), path, body));
        if ("GET".equals(r.getMethod())) {
            return path.contains("/state") || path.contains("/status") ? json(200, stateBody) : json(200, "{\"items\":[]}");
        }
        switch (mode) {
            case "SILENT" -> {
                return json(504, "{}");
            }
            case "ERROR" -> {
                return json(503, "{}");
            }
            case "RATE_LIMITED" -> {
                return json(429, "{}").setHeader("Retry-After", "7");
            }
            default -> {
            }
        }
        String id = r.getHeader(requestIdHeader);
        if (id != null) {
            perRequestId.computeIfAbsent(requestIdDecoder.apply(id), k -> new AtomicInteger()).incrementAndGet();
        }
        return json(200, commandResponse);
    }

    int effects(String commandId) {
        AtomicInteger n = perRequestId.get(commandId);
        return n == null ? 0 : n.get();
    }

    static MockResponse json(int status, String body) {
        return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body);
    }
}
