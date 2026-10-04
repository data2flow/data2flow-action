package net.java21.data2flow.action.actuation.driver.vendor;

import net.java21.data2flow.action.actuation.driver.DeviceDriver;
import net.java21.data2flow.action.actuation.driver.DriverCommand;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverDevice;
import net.java21.data2flow.action.actuation.driver.DriverEventSink;
import net.java21.data2flow.action.actuation.driver.DriverHealth;
import net.java21.data2flow.action.actuation.driver.DriverResult;
import net.java21.data2flow.action.actuation.driver.ReportedState;
import net.java21.data2flow.action.actuation.driver.StateListener;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import net.java21.data2flow.contracts.secret.Secret;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 클라우드 벤더 제어 드라이버의 공통 틀(ACT-03.04, ADR-040 "파사드 + 가짜 구현 + 계약 테스트"). 벤더 REST API를 부르는 일(인증 헤더, 401이면
 * 토큰을 한 번 갱신하고 다시, 429·5xx는 일시 실패로 재시도 대상, 같은 commandId는 한 번만)은 여기서 하고, 표준 기능 ↔ 벤더 요청·응답 매핑만
 * 하위 클래스가 한다.
 *
 * <p>키가 없어 실제 벤더 API로 검증하지 못했으므로(go-live O-11) 기본 꺼짐이고({@code data2flow.action.vendors.*.enabled=false}), 꺼져
 * 있으면 빈으로 만들지 않는다(드라이버 목록 "준비 중", 연결 확인은 DRIVER_NOT_FOUND). 매핑은 공개 문서의 예시 요청·응답으로 만든 계약
 * 테스트(MockWebServer)로만 확인한다. 실제 키가 생기면 실제 연동 테스트만 더한다.
 */
public abstract class CloudVendorDriver implements DeviceDriver {

    private final HttpClient http;
    private final String baseUrl;
    private final Duration timeout;
    protected final DriverEventSink sink;
    protected final Clock clock;
    /** 같은 commandId 재호출 방지(장비 효과 1회) */
    private final Map<UUID, DriverResult> done = new ConcurrentHashMap<>();
    /** 드라이버 ID → 갱신한 접근 토큰 */
    private final Map<Long, String> refreshedTokens = new ConcurrentHashMap<>();

    protected CloudVendorDriver(String baseUrl, Duration timeout, DriverEventSink sink, Clock clock) {
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.timeout = timeout;
        this.sink = sink;
        this.clock = clock;
    }

    /** 벤더 요청 하나 */
    protected record VendorRequest(String method, String path, Object body) {
    }

    /** 표준 명령 → 벤더 요청. 지원하지 않으면 {@link IllegalArgumentException} */
    protected abstract VendorRequest controlRequest(DriverCommand command);

    /** 상태 조회 경로 */
    protected abstract String statePath(String vendorDeviceId);

    /** 벤더 상태 응답 → 표준 상태 {@code {capability:{attr:value}}} */
    protected abstract Map<String, Map<String, Object>> toStandardState(JsonNode body);

    /** 연결 확인 경로 */
    protected abstract String healthPath(DriverConfig config);

    /** 벤더 공통 헤더(인증 제외) */
    protected Map<String, String> headers(DriverConfig config, UUID commandId) {
        return Map.of();
    }

    /** 토큰 갱신 경로(없으면 null = 갱신하지 않음) */
    protected String refreshPath() {
        return null;
    }

    @Override
    public DriverHealth healthCheck(DriverConfig config) {
        long started = System.nanoTime();
        try {
            HttpResponse<String> res = call(config, new VendorRequest("GET", healthPath(config), null), null);
            long ms = (System.nanoTime() - started) / 1_000_000;
            if (res.statusCode() == 200) {
                return DriverHealth.up(ms, supportedCapabilities());
            }
            String kind = res.statusCode() == 401 || res.statusCode() == 403 ? "AUTH" : "REFUSED";
            return DriverHealth.down(ms, supportedCapabilities(), kind, "HTTP " + res.statusCode());
        } catch (IOException e) {
            return DriverHealth.down((System.nanoTime() - started) / 1_000_000, supportedCapabilities(), "UNREACHABLE", e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DriverHealth.down(0, supportedCapabilities(), "UNREACHABLE", "중단됨");
        }
    }

    @Override
    public DriverResult execute(DriverCommand command) {
        DriverResult previous = done.get(command.commandId());
        if (previous != null) {
            return previous;
        }
        VendorRequest req;
        try {
            req = controlRequest(command);
        } catch (IllegalArgumentException e) {
            return DriverResult.failed(CommandStatusReasons.CAPABILITY_NOT_SUPPORTED, false, e.getMessage());
        }
        try {
            HttpResponse<String> res = call(command.device().config(), req, command.commandId());
            int status = res.statusCode();
            if (status >= 200 && status < 300) {
                DriverResult ok = new DriverResult(DriverResult.Status.ACKED, null, false, Map.of("vendorStatus", status));
                done.put(command.commandId(), ok);
                return ok;
            }
            if (status == 429 || status == 408 || status >= 500) {
                Map<String, Object> detail = new LinkedHashMap<>();
                detail.put("message", "벤더 API 일시 장애: HTTP " + status);
                res.headers().firstValue("Retry-After").ifPresent(v -> detail.put("retryAfterSec", v));
                return new DriverResult(DriverResult.Status.FAILED, CommandStatusReasons.DRIVER_ERROR, true, detail);
            }
            if (status == 401 || status == 403) {
                return DriverResult.failed("AUTH", false, "벤더 인증 실패: HTTP " + status);
            }
            return DriverResult.failed(CommandStatusReasons.INVALID_COMMAND, false, "벤더가 명령을 거부했습니다: HTTP " + status);
        } catch (IOException e) {
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, "중단됨");
        }
    }

    @Override
    public Optional<ReportedState> getState(DriverDevice device) {
        try {
            HttpResponse<String> res = call(device.config(), new VendorRequest("GET", statePath(device.externalId()), null), null);
            if (res.statusCode() != 200) {
                return Optional.empty();
            }
            Map<String, Map<String, Object>> state = toStandardState(Json.MAPPER.readTree(res.body()));
            return state.isEmpty() ? Optional.empty()
                    : Optional.of(new ReportedState(device.externalId(), state, clock.millis(), clock.instant()));
        } catch (IOException e) {
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    @Override
    public void subscribeState(DriverDevice device, StateListener listener) {
        // 벤더 푸시(웹훅·구독 이벤트)는 onEvent로 들어온다. 구독 등록은 키 발급 뒤 실제 어댑터에서 한다
    }

    /** 벤더 푸시를 표준 {@code device.state.reported}로 넘긴다(BR-ACT-25). 버전은 받은 시각(밀리초) */
    protected void report(long organizationId, long deviceId, Map<String, Map<String, Object>> state) {
        if (!state.isEmpty()) {
            sink.reported(organizationId, new DeviceStateReported(deviceId, clock.millis(), state, clock.instant(), false));
        }
    }

    private HttpResponse<String> call(DriverConfig config, VendorRequest req, UUID commandId) throws IOException, InterruptedException {
        HttpResponse<String> res = send(config, req, commandId, token(config));
        if (res.statusCode() == 401 && refreshPath() != null) {
            Optional<String> refreshed = refresh(config);
            if (refreshed.isPresent()) {
                res = send(config, req, commandId, refreshed.get());   // 토큰 만료: 갱신 뒤 1회만 다시
            }
        }
        return res;
    }

    private HttpResponse<String> send(DriverConfig config, VendorRequest req, UUID commandId, String token)
            throws IOException, InterruptedException {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(baseUrl + req.path())).timeout(timeout);
        headers(config, commandId).forEach(b::header);
        if (token != null) {
            b.header("Authorization", "Bearer " + token);
        }
        if (req.body() == null) {
            b.method(req.method(), HttpRequest.BodyPublishers.noBody());
        } else {
            b.header("Content-Type", "application/json").method(req.method(), HttpRequest.BodyPublishers.ofString(Json.write(req.body())));
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private String token(DriverConfig config) {
        String refreshed = config.driverId() == null ? null : refreshedTokens.get(config.driverId());
        if (refreshed != null) {
            return refreshed;
        }
        Secret t = config.secret("token");
        return t == null ? null : t.reveal();
    }

    private Optional<String> refresh(DriverConfig config) throws IOException, InterruptedException {
        Secret refreshToken = config.secret("refreshToken");
        if (refreshToken == null) {
            return Optional.empty();
        }
        HttpRequest r = HttpRequest.newBuilder(URI.create(baseUrl + refreshPath())).timeout(timeout)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString("grant_type=refresh_token&refresh_token="
                        + java.net.URLEncoder.encode(refreshToken.reveal(), java.nio.charset.StandardCharsets.UTF_8)))
                .build();
        HttpResponse<String> res = http.send(r, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 200) {
            return Optional.empty();
        }
        String token = Json.MAPPER.readTree(res.body()).path("access_token").asString(null);
        if (token != null && config.driverId() != null) {
            refreshedTokens.put(config.driverId(), token);
        }
        return Optional.ofNullable(token);
    }
}
