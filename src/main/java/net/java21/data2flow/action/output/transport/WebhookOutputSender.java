package net.java21.data2flow.action.output.transport;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.domain.DeliveryResult;
import net.java21.data2flow.action.output.domain.HostGuard;
import net.java21.data2flow.action.output.domain.OutboundMessage;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.contracts.output.OutputFailureKind;
import net.java21.data2flow.contracts.output.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 출력 연결 Webhook 발송(DSC-04.01, type=WEBHOOK). 배치 항목들을 JSON 배열 하나로 {@code target.method}(POST·PUT)한다.
 * <ul>
 *   <li>비밀값 HEADER_VALUE → {@code target.authHeaderName}(기본 Authorization) 헤더</li>
 *   <li>비밀값 HMAC_KEY → {@code X-D2F-Timestamp: 유닉스 초}, {@code X-D2F-Signature: v1=hex(HMAC-SHA256(키, 시각 + "." + 본문))}</li>
 *   <li>응답 2xx 성공, 401·403 AUTH, 그 밖 HTTP_STATUS. 이름 풀기 DNS, TLS, 시간 초과 TIMEOUT, 연결 거부 REFUSED</li>
 * </ul>
 * 공용 브로커 주소는 접속하지 않는다(CLAUDE.md §5).
 */
public class WebhookOutputSender {

    private static final Logger log = LoggerFactory.getLogger(WebhookOutputSender.class);
    /** JDK HttpClient가 직접 정하는 헤더(설정해도 무시) */
    private static final Set<String> RESTRICTED = Set.of("host", "content-length", "connection", "expect", "upgrade",
            "x-d2f-signature", "x-d2f-timestamp");

    private final HttpClient http;
    private final List<String> deniedHosts;
    private final Duration defaultTimeout;
    private final Clock clock;

    public WebhookOutputSender(List<String> deniedHosts, Duration defaultTimeout, Clock clock) {
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build();
        this.deniedHosts = List.copyOf(deniedHosts);
        this.defaultTimeout = defaultTimeout;
        this.clock = clock;
    }

    /** 배치 본문(JSON 배열). TEMPLATE 항목이 JSON이 아니면 문자열로 넣는다 */
    public static String batchBody(OutputConnection c, List<OutboundMessage> items) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String body = items.get(i).body();
            sb.append(c.format() == OutputFormat.TEMPLATE && !isJson(body) ? Json.write(body) : body);
        }
        return sb.append(']').toString();
    }

    static boolean isJson(String s) {
        try {
            Json.MAPPER.readTree(s);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public DeliveryResult send(OutputConnection c, List<OutboundMessage> items) {
        return post(c, batchBody(c, items));
    }

    public DeliveryResult post(OutputConnection c, String body) {
        if (HostGuard.denied(c.url(), deniedHosts)) {
            log.warn("출력 연결 {}의 Webhook 주소는 접속 금지입니다(CLAUDE.md §5). 보내지 않습니다", c.id());
            return DeliveryResult.failure(OutputFailureKind.REFUSED, "FORBIDDEN_HOST", null, null);
        }
        try {
            String method = c.target().path("method").asString("POST").toUpperCase(Locale.ROOT);
            int timeoutMs = c.target().path("timeoutMs").asInt((int) defaultTimeout.toMillis());
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(c.url().strip()))
                    .timeout(Duration.ofMillis(Math.max(1000, Math.min(timeoutMs, 30_000))))
                    .header("Content-Type", "application/json")
                    .method(method.equals("PUT") ? "PUT" : "POST", HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
            c.target().path("headers").properties().forEach(e -> {
                if (!RESTRICTED.contains(e.getKey().toLowerCase(Locale.ROOT)) && !e.getValue().isNull()) {
                    req.header(e.getKey(), e.getValue().asString());
                }
            });
            String headerValue = c.secrets().get("HEADER_VALUE");
            if (headerValue != null) {
                req.setHeader(c.target().path("authHeaderName").asString("Authorization"), headerValue);
            }
            String hmacKey = c.secrets().get("HMAC_KEY");
            if (hmacKey != null) {
                String ts = Long.toString(clock.instant().getEpochSecond());
                req.header("X-D2F-Timestamp", ts);
                req.header("X-D2F-Signature", "v1=" + hmac(hmacKey, ts + "." + body));
            }
            HttpResponse<String> res = http.send(req.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            String preview = preview(res.body());
            int status = res.statusCode();
            if (status >= 200 && status < 300) {
                return DeliveryResult.success(status, preview);
            }
            OutputFailureKind kind = status == 401 || status == 403 ? OutputFailureKind.AUTH : OutputFailureKind.HTTP_STATUS;
            return DeliveryResult.failure(kind, "HTTP " + status, status, preview);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DeliveryResult.failure(OutputFailureKind.TIMEOUT, "interrupted", null, null);
        } catch (Exception e) {
            return DeliveryResult.failure(FailureKinds.of(e), FailureKinds.describe(e), null, null);
        }
    }

    /** 서명 값(소문자 hex 64자) */
    public static String hmac(String key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static String preview(String body) {
        if (body == null) {
            return null;
        }
        return body.length() > 1024 ? body.substring(0, 1024) : body;
    }
}
