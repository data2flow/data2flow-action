package net.java21.data2flow.action.sink.connector.influx;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.sink.connector.ErrorKind;
import net.java21.data2flow.action.sink.connector.Identifiers;
import net.java21.data2flow.action.sink.connector.SinkBatch;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnector;
import net.java21.data2flow.action.sink.connector.SinkConnectorVerified;
import net.java21.data2flow.action.sink.connector.SinkTestResult;
import net.java21.data2flow.action.sink.connector.SinkWriteException;
import net.java21.data2flow.action.sink.connector.TargetSchema;
import net.java21.data2flow.contracts.sink.SinkTypes;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB 2 Sink(FLW-04.02). HTTP 라인 프로토콜({@code POST /api/v2/write}, {@code Authorization: Token})로 쓰고, 추가 라이브러리는 쓰지 않는다.
 *
 * <ul>
 *   <li>대상(target) = measurement. 레코드의 {@code time}·{@code ts}·{@code timestamp} 중 처음 있는 키가 점 시각(ISO-8601 또는 epoch ms)이고,
 *       {@code upsertKeys}는 태그(시리즈 식별), 나머지는 필드다. 시각이 없으면 받은 시각(Clock)을 쓴다.</li>
 *   <li>정확히 한 번: InfluxDB는 같은 measurement·태그·시각의 점을 덮어쓰므로 같은 배치를 다시 써도 결과가 같다(자연 멱등). 그래서 표시 행이 없다.</li>
 *   <li>스키마가 없는 저장소라 스키마 확인은 버킷 존재만 보고, 자동 생성은 하지 않는다(버킷은 운영자가 만든다).</li>
 * </ul>
 * 응답: 2xx 성공, 401·403 AUTH(영구), 404 버킷 없음(TARGET 영구), 400·413·422 TARGET(영구), 429·5xx·연결 오류 일시 실패.
 */
@SinkConnectorVerified("InfluxSinkConnectorContractTest")
public class InfluxSinkConnector implements SinkConnector {

    private static final List<String> TIME_KEYS = List.of("time", "ts", "timestamp");

    private final HttpClient http;
    private final Clock clock;
    private final Duration requestTimeout;

    public InfluxSinkConnector(Clock clock, Duration connectTimeout) {
        this.clock = clock;
        this.requestTimeout = Duration.ofSeconds(30);
        this.http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(connectTimeout).build();
    }

    @Override
    public String type() {
        return SinkTypes.INFLUXDB;
    }

    @Override
    public SinkTestResult test(SinkConnection c) {
        long start = System.nanoTime();
        try {
            bucketExists(c);
            return SinkTestResult.ok(Duration.ofNanos(System.nanoTime() - start).toMillis());
        } catch (SinkWriteException e) {
            return SinkTestResult.failed(Duration.ofNanos(System.nanoTime() - start).toMillis(), e.kind(), e.getMessage());
        }
    }

    @Override
    public TargetSchema describe(SinkConnection c, String target) throws SinkWriteException {
        Identifiers.require(target);
        return bucketExists(c) ? new TargetSchema(true, List.of(), true) : TargetSchema.missing();
    }

    @Override
    public void create(SinkConnection c, String target, List<TargetSchema.Column> columns, List<String> primaryKey)
            throws SinkWriteException {
        Identifiers.require(target);
        if (!bucketExists(c)) {
            throw new SinkWriteException(ErrorKind.TARGET, false, "버킷이 없습니다: " + c.string("bucket", ""));
        }
        // measurement는 첫 쓰기 때 생긴다(스키마 없음)
    }

    @Override
    public WriteOutcome write(SinkConnection c, SinkBatch batch) throws SinkWriteException {
        String body = lines(batch);
        String uri = base(c) + "/api/v2/write?org=" + enc(c.string("org", "")) + "&bucket=" + enc(c.string("bucket", ""))
                + "&precision=ms";
        HttpRequest req = HttpRequest.newBuilder(URI.create(uri)).timeout(requestTimeout)
                .header("Authorization", "Token " + token(c))
                .header("Content-Type", "text/plain; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> res = send(req);
        check(res);
        return WriteOutcome.WRITTEN;
    }

    /** 라인 프로토콜 본문 */
    public String lines(SinkBatch batch) throws SinkWriteException {
        String measurement = escapeKey(Identifiers.require(batch.target()));
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> r : batch.records()) {
            String timeKey = TIME_KEYS.stream().filter(r::containsKey).findFirst().orElse(null);
            StringBuilder line = new StringBuilder(measurement);
            for (String tag : batch.upsertKeys()) {
                if (tag.equals(timeKey) || r.get(tag) == null) {
                    continue;
                }
                line.append(',').append(escapeKey(Identifiers.require(tag))).append('=').append(escapeKey(r.get(tag).toString()));
            }
            StringBuilder fields = new StringBuilder();
            for (Map.Entry<String, Object> e : r.entrySet()) {
                if (e.getKey().equals(timeKey) || batch.upsertKeys().contains(e.getKey()) || e.getValue() == null) {
                    continue;
                }
                fields.append(fields.isEmpty() ? "" : ",").append(escapeKey(Identifiers.require(e.getKey()))).append('=')
                        .append(fieldValue(e.getValue()));
            }
            if (fields.isEmpty()) {
                throw new SinkWriteException(ErrorKind.TARGET, false, "필드가 없는 레코드는 쓸 수 없습니다");
            }
            line.append(' ').append(fields).append(' ').append(epochMillis(timeKey == null ? null : r.get(timeKey)));
            sb.append(line).append('\n');
        }
        return sb.toString();
    }

    private long epochMillis(Object v) throws SinkWriteException {
        if (v == null) {
            return clock.instant().toEpochMilli();
        }
        if (v instanceof Number n) {
            return n.longValue();
        }
        try {
            return OffsetDateTime.parse(v.toString()).toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            try {
                return Instant.parse(v.toString()).toEpochMilli();
            } catch (DateTimeParseException e2) {
                throw new SinkWriteException(ErrorKind.TARGET, false, "시각 형식이 아닙니다: " + v);
            }
        }
    }

    private static String fieldValue(Object v) {
        if (v instanceof Boolean b) {
            return b.toString();
        }
        if (v instanceof Integer || v instanceof Long || v instanceof Short) {
            return v + "i";
        }
        if (v instanceof Number n) {
            return Double.toString(n.doubleValue());
        }
        String s = v instanceof Map<?, ?> || v instanceof List<?> ? Json.write(v) : v.toString();
        return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    private static String escapeKey(String s) {
        return s.replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ");
    }

    private boolean bucketExists(SinkConnection c) throws SinkWriteException {
        String uri = base(c) + "/api/v2/buckets?name=" + enc(c.string("bucket", "")) + "&org=" + enc(c.string("org", ""));
        HttpRequest req = HttpRequest.newBuilder(URI.create(uri)).timeout(requestTimeout)
                .header("Authorization", "Token " + token(c)).GET().build();
        HttpResponse<String> res = send(req);
        if (res.statusCode() == 404) {
            return false;   // 조직이 없을 때
        }
        check(res);
        JsonNode body = Json.MAPPER.readTree(res.body());
        return body.path("buckets").size() > 0;
    }

    private HttpResponse<String> send(HttpRequest req) throws SinkWriteException {
        try {
            // JDK HttpClient는 이름 풀이 실패도 ConnectException으로 감싸므로 먼저 풀어 DNS를 구분한다
            java.net.InetAddress.getByName(req.uri().getHost());
        } catch (UnknownHostException e) {
            throw new SinkWriteException(ErrorKind.DNS, true, "주소를 찾을 수 없습니다: " + req.uri().getHost(), e);
        }
        try {
            return http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (HttpConnectTimeoutException e) {
            throw new SinkWriteException(ErrorKind.TIMEOUT, true, "연결 시간 초과", e);
        } catch (HttpTimeoutException e) {
            throw new SinkWriteException(ErrorKind.TIMEOUT, true, "응답 시간 초과", e);
        } catch (IOException e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof UnknownHostException) {
                    throw new SinkWriteException(ErrorKind.DNS, true, "주소를 찾을 수 없습니다: " + req.uri().getHost(), e);
                }
                if (t instanceof javax.net.ssl.SSLException) {
                    throw new SinkWriteException(ErrorKind.TLS, false, "TLS 오류: " + t.getMessage(), e);
                }
                if (t instanceof ConnectException) {
                    throw new SinkWriteException(ErrorKind.REFUSED, true, "연결 거부: " + req.uri().getHost(), e);
                }
            }
            throw new SinkWriteException(ErrorKind.REFUSED, true, "연결 오류: " + e, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SinkWriteException(ErrorKind.OTHER, true, "중단됨", e);
        }
    }

    private static void check(HttpResponse<String> res) throws SinkWriteException {
        int s = res.statusCode();
        if (s >= 200 && s < 300) {
            return;
        }
        String detail = "HTTP " + s + (res.body() == null ? "" : " " + truncate(res.body()));
        if (s == 401 || s == 403) {
            throw new SinkWriteException(ErrorKind.AUTH, false, detail);
        }
        if (s == 429 || s >= 500) {
            throw new SinkWriteException(ErrorKind.OTHER, true, detail);
        }
        throw new SinkWriteException(ErrorKind.TARGET, false, detail);
    }

    private static String base(SinkConnection c) {
        String url = c.string("url", null);
        if (url == null) {
            url = (c.flag("ssl") ? "https://" : "http://") + c.string("host", "localhost") + ":" + c.integer("port", 8086);
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static String token(SinkConnection c) {
        String t = c.secret("token");
        return t == null ? "" : t;
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String truncate(String s) {
        return s.length() > 300 ? s.substring(0, 300) : s;
    }
}
