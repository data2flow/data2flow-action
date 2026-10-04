package net.java21.data2flow.action.notification.channel.telegram;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.channel.CallbackAcknowledger;
import net.java21.data2flow.action.notification.channel.WebhookRegistrar;
import net.java21.data2flow.contracts.notification.CallbackAction;
import net.java21.data2flow.contracts.notification.CallbackCommand;
import net.java21.data2flow.contracts.notification.ChannelButton;
import net.java21.data2flow.contracts.notification.ChannelCapabilities;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.ChannelSettings;
import net.java21.data2flow.contracts.notification.InboundRequest;
import net.java21.data2flow.contracts.notification.LinkRequest;
import net.java21.data2flow.contracts.notification.LinkResult;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import net.java21.data2flow.contracts.notification.SendResult;
import net.java21.data2flow.contracts.secret.Secret;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 텔레그램 알림 채널(OPS-06.01, ADR-033): Bot API {@code sendMessage}·{@code editMessageText}(MarkdownV2 + 인라인 버튼),
 * 콜백 시크릿 검증({@code X-Telegram-Bot-Api-Secret-Token}, 상수 시간 비교), 버튼 응답·{@code /start {코드}} 변환, 딥링크
 * {@code https://t.me/{botUsername}?start={code}}, 저장 때 {@code setWebhook}.
 *
 * <p>비밀값: 봇 토큰({@code botToken})과 웹훅 시크릿({@code webhookSecret})은 core가 채널 정의와 함께 준다(API-OPS-35). 토큰은 Bot API 주소
 * 경로에 들어가므로 오류 문구에 주소를 넣지 않는다.
 *
 * <p>실제 봇 토큰이 아직 없어서 기본은 꺼짐({@code data2flow.action.notification.telegram.enabled=false}): {@link #available()}이 false이고
 * 발송은 영구 실패 {@code CHANNEL_UNAVAILABLE}로 끝난다. 시험은 MockWebServer로만 부른다(api.telegram.org 호출 금지).
 */
public class TelegramChannel implements NotificationChannel, WebhookRegistrar, CallbackAcknowledger {

    public static final String KEY = "TELEGRAM";
    public static final String SECRET_HEADER = "X-Telegram-Bot-Api-Secret-Token";
    public static final String BOT_TOKEN = "botToken";
    public static final String WEBHOOK_SECRET = "webhookSecret";
    private static final String MD_SPECIAL = "_*[]()~`>#+-=|{}.!\\";
    private static final Logger log = LoggerFactory.getLogger(TelegramChannel.class);

    private final NotificationProperties.Telegram properties;
    private final RestClient http;

    public TelegramChannel(NotificationProperties.Telegram properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(3));
        factory.setReadTimeout(Duration.ofSeconds(10));
        this.http = RestClient.builder().baseUrl(properties.apiBaseUrl()).requestFactory(factory).build();
    }

    @Override
    public String key() {
        return KEY;
    }

    @Override
    public boolean available() {
        return properties.enabled();
    }

    @Override
    public JsonNode configSchema() {
        return Json.MAPPER.readTree("""
                {"type":"object","required":["chatIds"],"additionalProperties":false,
                 "properties":{
                   "chatIds":{"type":"array","minItems":1,"items":{"type":"string","pattern":"^-?[0-9]+$"},"title":"기본 채팅방 chat_id"},
                   "botUsername":{"type":"string","pattern":"^[A-Za-z0-9_]{5,32}$","title":"봇 사용자 이름(계정 연결 딥링크)"},
                   "parseMode":{"enum":["MarkdownV2","PLAIN"],"default":"MarkdownV2","title":"메시지 형식"}}}""");
    }

    @Override
    public ChannelCapabilities capabilities() {
        return new ChannelCapabilities(true, Set.of(ChannelCapabilities.BodyFormat.PLAIN, ChannelCapabilities.BodyFormat.MARKDOWN_V2), 4000,
                20, 15);
    }

    @Override
    public SendResult send(ChannelSettings settings, ChannelMessage message) {
        if (!available()) {
            return SendResult.permanentFailure("CHANNEL_UNAVAILABLE: 텔레그램 어댑터가 꺼져 있습니다");
        }
        Secret token = settings.secret(BOT_TOKEN);
        if (token == null || token.isEmpty()) {
            return SendResult.permanentFailure("BOT_TOKEN_MISSING: 봇 토큰이 설정되지 않았습니다");
        }
        boolean markdown = !"PLAIN".equals(settings.config().path("parseMode").asString("MarkdownV2"));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("chat_id", message.recipientAddress());
        body.put("text", text(message, markdown));
        if (markdown) {
            body.put("parse_mode", "MarkdownV2");
        }
        body.put("disable_web_page_preview", true);
        if (!message.buttons().isEmpty()) {
            List<Map<String, String>> row = new ArrayList<>();
            for (ChannelButton b : message.buttons()) {
                row.add(Map.of("text", b.label(), "callback_data", b.callbackData()));
            }
            body.put("reply_markup", Map.of("inline_keyboard", List.of(row)));
        } else if (message.replaceExternalMessageId() != null) {
            body.put("reply_markup", Map.of("inline_keyboard", List.of()));
        }
        String method = "sendMessage";
        if (message.replaceExternalMessageId() != null) {
            method = "editMessageText";
            body.put("message_id", parseMessageId(message.replaceExternalMessageId()));
        }
        Reply reply = call(token, method, body);
        SendResult.Outcome outcome = SendResult.classifyHttpStatus(reply.status());
        if (outcome == SendResult.Outcome.SUCCESS && reply.body().path("ok").asBoolean(false)) {
            JsonNode result = reply.body().path("result");
            String id = result.path("message_id").isMissingNode() ? message.replaceExternalMessageId() : result.path("message_id").asString();
            return SendResult.success(id == null ? "edited" : id);
        }
        String description = reply.body().path("description").asString("HTTP " + reply.status());
        if (outcome == SendResult.Outcome.SUCCESS) {
            return SendResult.permanentFailure("telegram: " + description);
        }
        if (outcome == SendResult.Outcome.TRANSIENT_FAILURE) {
            JsonNode retry = reply.body().path("parameters").path("retry_after");
            return SendResult.transientFailure("HTTP " + reply.status() + " " + description,
                    retry.isNumber() ? Duration.ofSeconds(retry.asLong()) : null);
        }
        return SendResult.permanentFailure("HTTP " + reply.status() + " " + description);
    }

    @Override
    public boolean verify(ChannelSettings settings, InboundRequest request) {
        Secret expected = settings.secret(WEBHOOK_SECRET);
        String given = request.header(SECRET_HEADER);
        if (expected == null || expected.isEmpty() || given == null) {
            return false;
        }
        return MessageDigest.isEqual(expected.reveal().getBytes(StandardCharsets.UTF_8), given.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Optional<CallbackCommand> handleCallback(ChannelSettings settings, InboundRequest request) {
        JsonNode update;
        try {
            update = Json.MAPPER.readTree(request.bodyAsString());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        JsonNode cq = update.path("callback_query");
        if (cq.isObject()) {
            String from = cq.path("from").path("id").asString("");
            String data = cq.path("data").asString("");
            Optional<CallbackAction> action = CallbackAction.fromCallbackData(data);
            if (from.isBlank() || action.isEmpty()) {
                return Optional.empty();
            }
            String[] parts = data.split("\\|", -1);
            Long alarmId = parts.length > 1 && parts[1].matches("\\d+") ? Long.parseLong(parts[1]) : null;
            String deliveryId = parts.length > 2 && !parts[2].isEmpty() ? parts[2] : null;
            JsonNode msg = cq.path("message");
            String messageId = msg.path("message_id").isMissingNode() ? null : msg.path("message_id").asString();
            return Optional.of(CallbackCommand.button(action.get(), from, alarmId, deliveryId, messageId, cq.path("id").asString(null)));
        }
        JsonNode msg = update.path("message");
        String text = msg.path("text").asString("").trim();
        String from = msg.path("from").path("id").asString("");
        if (!from.isBlank() && text.startsWith("/start ")) {
            String code = text.substring("/start ".length()).trim();
            return code.isEmpty() ? Optional.empty() : Optional.of(CallbackCommand.link(from, code));
        }
        return Optional.empty();
    }

    @Override
    public LinkResult link(ChannelSettings settings, LinkRequest request) {
        String bot = settings.config().path("botUsername").asString("data2flow_bot");
        return new LinkResult("https://t.me/" + bot + "?start=" + request.code(), request.expiresAt());
    }

    @Override
    public String registerWebhook(ChannelSettings settings, String url) {
        Secret token = settings.secret(BOT_TOKEN);
        Secret secret = settings.secret(WEBHOOK_SECRET);
        if (!available()) {
            return "CHANNEL_UNAVAILABLE";
        }
        if (token == null || secret == null) {
            return "BOT_TOKEN_MISSING";
        }
        Reply r = call(token, "setWebhook", Map.of("url", url, "secret_token", secret.reveal(),
                "allowed_updates", List.of("message", "callback_query")));
        return r.status() == 200 && r.body().path("ok").asBoolean(false) ? null
                : "HTTP " + r.status() + " " + r.body().path("description").asString("");
    }

    @Override
    public void acknowledge(ChannelSettings settings, CallbackCommand command, String text) {
        Secret token = settings.secret(BOT_TOKEN);
        if (!available() || token == null || command.callbackId() == null) {
            return;
        }
        Reply r = call(token, "answerCallbackQuery", Map.of("callback_query_id", command.callbackId(), "text", text == null ? "" : text));
        if (r.status() != 200) {
            log.debug("answerCallbackQuery 실패 HTTP {}", r.status());
        }
    }

    // ───────────── 도움 ─────────────

    private record Reply(int status, JsonNode body) {
    }

    private Reply call(Secret token, String method, Object body) {
        try {
            return http.post().uri("/bot" + token.reveal() + "/" + method)   // 변수 확장은 ":"를 인코딩하므로 그대로 붙인다
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Json.write(body))
                    .exchange((req, res) -> {
                        byte[] bytes = res.getBody().readAllBytes();
                        JsonNode json;
                        try {
                            json = bytes.length == 0 ? Json.MAPPER.createObjectNode() : Json.MAPPER.readTree(bytes);
                        } catch (RuntimeException e) {
                            json = Json.MAPPER.createObjectNode();
                        }
                        return new Reply(res.getStatusCode().value(), json);
                    });
        } catch (RuntimeException e) {
            // 연결 실패: 주소에 토큰이 있으므로 원인 종류만 남긴다(일시 실패로 다시 시도)
            Throwable cause = e.getCause() == null ? e : e.getCause();
            return new Reply(503, Json.MAPPER.createObjectNode().put("description", "I/O " + cause.getClass().getSimpleName()));
        }
    }

    static String text(ChannelMessage m, boolean markdown) {
        StringBuilder sb = new StringBuilder();
        if (m.title() != null && !m.title().isBlank() && !m.body().startsWith(m.title())) {
            sb.append(markdown ? "*" + escape(m.title()) + "*" : m.title()).append("\n\n");
        }
        sb.append(markdown ? escape(m.body()) : m.body());
        if (m.link() != null && !m.body().contains(m.link())) {
            sb.append("\n").append(markdown ? escape(m.link()) : m.link());
        }
        return sb.toString();
    }

    /** MarkdownV2 예약 문자 이스케이프(Bot API "Formatting options") */
    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (char c : s.toCharArray()) {
            if (MD_SPECIAL.indexOf(c) >= 0) {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static Object parseMessageId(String id) {
        try {
            return Long.parseLong(id);
        } catch (NumberFormatException e) {
            return id;
        }
    }
}
