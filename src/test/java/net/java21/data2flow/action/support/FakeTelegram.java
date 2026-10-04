package net.java21.data2flow.action.support;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.notification.InboundRequest;
import net.java21.data2flow.contracts.test.notification.ChannelTestPeer;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 텔레그램 Bot API 대역(MockWebServer, testing/backend.md "외부 HTTP 대역"). 실제 api.telegram.org는 부르지 않는다.
 * {@code /bot{token}/{method}}에 Bot API 모양으로 답하고 받은 요청을 기록한다. 채널 계약 키트의 상대({@link ChannelTestPeer})이기도 하다.
 */
public final class FakeTelegram extends Dispatcher implements ChannelTestPeer {

    public static final String TOKEN = "123456:TEST-TOKEN";
    public static final String SECRET = "tg-webhook-secret";
    private static final Pattern PATH = Pattern.compile("/bot([^/]+)/(\\w+)");

    public final MockWebServer server = new MockWebServer();
    public final List<Call> calls = new CopyOnWriteArrayList<>();
    private final Deque<Object[]> replies = new ArrayDeque<>();
    private final AtomicInteger delivered = new AtomicInteger();
    private final AtomicInteger messageIds = new AtomicInteger(100);

    /** 받은 호출 */
    public record Call(String method, String token, JsonNode body) {
    }

    public FakeTelegram() {
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

    public synchronized void reset() {
        calls.clear();
        replies.clear();
        delivered.set(0);
    }

    /** 다음 sendMessage·editMessageText 응답(상태, retry_after) */
    public synchronized void nextStatus(int status, Integer retryAfter) {
        replies.add(new Object[]{status, retryAfter});
    }

    public List<Call> calls(String method) {
        return calls.stream().filter(c -> c.method().equals(method)).toList();
    }

    @Override
    public MockResponse dispatch(RecordedRequest request) {
        Matcher m = PATH.matcher(request.getPath() == null ? "" : request.getPath());
        if (!m.matches()) {
            return new MockResponse().setResponseCode(404);
        }
        String body = request.getBody().readUtf8();
        JsonNode json = body.isEmpty() ? Json.MAPPER.createObjectNode() : Json.MAPPER.readTree(body);
        calls.add(new Call(m.group(2), m.group(1), json));
        if (!TOKEN.equals(m.group(1))) {
            return reply(401, Map.of("ok", false, "error_code", 401, "description", "Unauthorized"));
        }
        return switch (m.group(2)) {
            case "sendMessage", "editMessageText" -> message();
            default -> reply(200, Map.of("ok", true, "result", true));
        };
    }

    private synchronized MockResponse message() {
        Object[] r = replies.isEmpty() ? new Object[]{200, null} : replies.poll();
        int status = (int) r[0];
        if (status == 200) {
            delivered.incrementAndGet();
            return reply(200, Map.of("ok", true, "result", Map.of("message_id", messageIds.incrementAndGet())));
        }
        Map<String, Object> err = r[1] == null
                ? Map.of("ok", false, "error_code", status, "description", "error " + status)
                : Map.of("ok", false, "error_code", status, "description", "Too Many Requests", "parameters", Map.of("retry_after", r[1]));
        MockResponse res = reply(status, err);
        return r[1] == null ? res : res.setHeader("Retry-After", r[1].toString());
    }

    private static MockResponse reply(int status, Object body) {
        return new MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(Json.write(body));
    }

    // ───────────── ChannelTestPeer ─────────────

    @Override
    public void nextReply(Reply reply) {
        switch (reply) {
            case OK -> nextStatus(200, null);
            case TRANSIENT -> nextStatus(429, 1);
            case PERMANENT -> nextStatus(403, null);
        }
    }

    @Override
    public int delivered() {
        return delivered.get();
    }

    @Override
    public InboundRequest callback(String callbackData, String externalUserId) {
        return inbound(SECRET, callbackJson(callbackData, externalUserId, 101));
    }

    @Override
    public InboundRequest callbackWithWrongSecret(String callbackData, String externalUserId) {
        return inbound(SECRET + "-wrong", callbackJson(callbackData, externalUserId, 101));
    }

    @Override
    public InboundRequest linkStart(String code, String externalUserId) {
        return inbound(SECRET, "{\"update_id\":2,\"message\":{\"message_id\":5,\"from\":{\"id\":" + externalUserId
                + "},\"chat\":{\"id\":" + externalUserId + ",\"type\":\"private\"},\"text\":\"/start " + code + "\"}}");
    }

    public static String callbackJson(String data, String externalUserId, int messageId) {
        return "{\"update_id\":1,\"callback_query\":{\"id\":\"cq-1\",\"from\":{\"id\":" + externalUserId + "},\"message\":{\"message_id\":"
                + messageId + ",\"chat\":{\"id\":" + externalUserId + "}},\"data\":\"" + data + "\"}}";
    }

    public static InboundRequest inbound(String secret, String body) {
        return new InboundRequest(Map.of("X-Telegram-Bot-Api-Secret-Token", secret), body.getBytes(StandardCharsets.UTF_8));
    }
}
