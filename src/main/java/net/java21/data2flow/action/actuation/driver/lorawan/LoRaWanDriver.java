package net.java21.data2flow.action.actuation.driver.lorawan;

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
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.contracts.secret.Secret;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * LoRaWAN 다운링크 드라이버(ACT-03.03, ADR-001: LoRaWAN은 ChirpStack에 맡긴다). 표준 명령을 {@link DownlinkEncoder}로 fPort·바이트로
 * 바꿔 ChirpStack v4 REST API {@code POST /api/devices/{dev-eui}/queue}에 등록한다(헤더 {@code Grpc-Metadata-Authorization: Bearer
 * {apiToken}}). 등록 응답의 큐 항목 ID는 결과 요약({@code queueItemId}·{@code confirmed})으로 돌려주고 창구가 명령 행
 * ({@code commands.downlink_queue_item_id})에 남긴다. ChirpStack 다운링크 결과는 ingress가 EVT-ACT-09 {@code lorawan.downlink.ack}로
 * 내고, action이 그 큐 항목 ID로 명령을 찾아 {@link #commandAck}로 표준 {@code device.command.ack}로 바꾼다(BR-ACT-25, ADR-054 남은 것 ①).
 * Class A 기기는 창구가 다음 업링크 직후에 이 드라이버를 부른다(ACT-07.02).
 *
 * <p>⏸ 안전(CLAUDE.md §5): 이 드라이버는 {@code data2flow.action.lorawan.enabled=true}일 때만 만들어지고(기본 꺼짐), 호출할 때마다
 * {@link ChirpStackHostGuard}로 공용 ChirpStack(s3.java21.net)·공용 브로커 주소를 거부한다. 시험은 ChirpStack API 목(MockWebServer)만 쓴다.
 * 설정: {@code {chirpstackUrl, applicationId, fPortDefault?, confirmed:true}} + 비밀값 {@code apiToken}. 기기 외부 ID는 DevEUI.
 */
public class LoRaWanDriver implements DeviceDriver {

    public static final String TYPE = "LORAWAN";
    private static final Logger log = LoggerFactory.getLogger(LoRaWanDriver.class);
    private static final int MAX_REMEMBERED = 10_000;
    /** 확인형 다운링크를 기기가 확인하지 않았다(ChirpStack ack {@code acknowledged=false}) */
    public static final String NOT_ACKNOWLEDGED = "DOWNLINK_NOT_ACKNOWLEDGED";

    private final HttpClient http;
    private final Duration timeout;
    private final List<String> deniedHosts;
    private final DriverEventSink sink;
    private final Clock clock;
    /** 명령 ID → 등록한 큐 항목(같은 commandId 재호출 시 다시 등록하지 않음. ack 조회는 DB가 맡는다) */
    private final Map<UUID, Queued> queued = java.util.Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, false) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, Queued> eldest) {
            return size() > MAX_REMEMBERED;
        }
    });

    private record Queued(String queueItemId, boolean confirmed) {
    }

    public LoRaWanDriver(Duration timeout, List<String> deniedHosts, DriverEventSink sink, Clock clock) {
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
        this.timeout = timeout;
        this.deniedHosts = deniedHosts == null ? List.of() : List.copyOf(deniedHosts);
        this.sink = sink;
        this.clock = clock;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public java.util.Set<String> supportedCapabilities() {
        return DownlinkEncoder.CAPABILITIES;
    }

    @Override
    public DriverHealth healthCheck(DriverConfig config) {
        long started = System.nanoTime();
        try {
            String base = baseUrl(config);
            String app = config.string("applicationId", null);
            if (app == null) {
                return DriverHealth.down(0, supportedCapabilities(), "CONFIG", "applicationId가 없습니다");
            }
            HttpResponse<String> res = send(config, HttpRequest.newBuilder(URI.create(base + "/api/applications/" + app)).GET());
            long ms = (System.nanoTime() - started) / 1_000_000;
            return switch (res.statusCode()) {
                case 200 -> DriverHealth.up(ms, supportedCapabilities());
                case 401, 403 -> DriverHealth.down(ms, supportedCapabilities(), "AUTH", "ChirpStack API 키가 거부되었습니다: HTTP " + res.statusCode());
                case 404 -> DriverHealth.down(ms, supportedCapabilities(), "NOT_FOUND", "애플리케이션이 없습니다: " + app);
                default -> DriverHealth.down(ms, supportedCapabilities(), "REFUSED", "HTTP " + res.statusCode());
            };
        } catch (IllegalStateException e) {
            return DriverHealth.down(0, supportedCapabilities(), "REFUSED", e.getMessage());
        } catch (IOException e) {
            return DriverHealth.down((System.nanoTime() - started) / 1_000_000, supportedCapabilities(), "UNREACHABLE", e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DriverHealth.down(0, supportedCapabilities(), "UNREACHABLE", "중단됨");
        }
    }

    @Override
    public DriverResult execute(DriverCommand command) {
        Queued previous = queued.get(command.commandId());
        if (previous != null) {
            return accepted(previous);   // 같은 commandId: 이미 큐에 있다(장비 효과 1회)
        }
        DriverConfig config = command.device().config();
        DownlinkEncoder.Downlink downlink;
        try {
            Object port = config.config().get("fPortDefault");
            downlink = DownlinkEncoder.encode(command.capability(), command.command(), command.args(),
                    port instanceof Number n ? n.intValue() : null);
        } catch (IllegalArgumentException e) {
            return DriverResult.failed(CommandStatusReasons.CAPABILITY_NOT_SUPPORTED, false, e.getMessage());
        }
        String devEui = command.device().externalId();
        if (devEui == null || devEui.isBlank()) {
            return DriverResult.failed(CommandStatusReasons.INVALID_COMMAND, false, "기기 외부 ID(DevEUI)가 없습니다");
        }
        boolean confirmed = !Boolean.FALSE.equals(config.config().get("confirmed"));
        Map<String, Object> body = Map.of("queueItem", Map.of("confirmed", confirmed, "fPort", downlink.fPort(),
                "data", Base64.getEncoder().encodeToString(downlink.bytes())));
        try {
            HttpResponse<String> res = send(config, HttpRequest.newBuilder(URI.create(baseUrl(config) + "/api/devices/" + devEui + "/queue"))
                    .header("Content-Type", "application/json")
                    .header("X-Request-Id", command.commandId().toString())
                    .POST(HttpRequest.BodyPublishers.ofString(Json.write(body))));
            int status = res.statusCode();
            if (status >= 200 && status < 300) {
                JsonNode json = Json.MAPPER.readTree(res.body().isBlank() ? "{}" : res.body());
                String queueId = json.path("id").asString(command.commandId().toString());
                Queued q = new Queued(queueId, confirmed);
                queued.put(command.commandId(), q);
                return accepted(q);
            }
            if (status == 401 || status == 403) {
                return DriverResult.failed("AUTH", false, "ChirpStack API 키가 거부되었습니다: HTTP " + status);
            }
            if (status == 404) {
                return DriverResult.failed(CommandStatusReasons.INVALID_COMMAND, false, "ChirpStack에 기기가 없습니다: " + devEui);
            }
            if (status == 408 || status == 429 || status >= 500) {
                return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, "ChirpStack 일시 장애: HTTP " + status);
            }
            return DriverResult.failed(CommandStatusReasons.INVALID_COMMAND, false, "ChirpStack이 다운링크를 거부했습니다: HTTP " + status);
        } catch (IllegalStateException e) {
            return DriverResult.failed(CommandStatusReasons.DRIVER_UNAVAILABLE, false, e.getMessage());
        } catch (IOException e) {
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, e.toString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, "중단됨");
        }
    }

    /**
     * EVT-ACT-09 다운링크 결과(ChirpStack {@code event/ack}·{@code event/txack})를 표준 {@code device.command.ack}로 바꾼다(BR-ACT-25).
     * <ul>
     *   <li>ACK: 기기가 확인했으면 ACKED, 확인하지 않았으면 FAILED({@value #NOT_ACKNOWLEDGED}).</li>
     *   <li>TXACK: 비확인형(unconfirmed) 다운링크는 게이트웨이 송신이 마지막 신호라 ACKED. 확인형은 기기 확인(ACK)을 기다리므로 바꾸지 않는다.</li>
     *   <li>모르는 종류는 바꾸지 않는다.</li>
     * </ul>
     *
     * @param commandId 큐 항목 ID로 찾은 명령(DB {@code commands.downlink_queue_item_id})
     * @param confirmed 등록할 때 확인형이었는가
     */
    public static Optional<DeviceCommandAck> commandAck(UUID commandId, long deviceId, LoRaWanDownlinkAck ack, boolean confirmed) {
        return switch (ack.effectiveKind()) {
            case ACK -> Optional.of(ack.acknowledged()
                    ? DeviceCommandAck.acked(commandId.toString(), deviceId, ack.at(), false)
                    : DeviceCommandAck.failed(commandId.toString(), deviceId, NOT_ACKNOWLEDGED, ack.at(), false));
            case TXACK -> confirmed ? Optional.empty() : Optional.of(DeviceCommandAck.acked(commandId.toString(), deviceId, ack.at(), false));
            case UNKNOWN -> Optional.empty();
        };
    }

    /**
     * ChirpStack 업링크 이벤트({@code event/up}의 디코드된 {@code object})를 표준 {@code device.state.reported}로 넘긴다. 버전은 업링크
     * 프레임 카운터(fCnt)라 오래된 업링크는 버려진다(BR-ACT-05). 업링크는 Class A 다운링크를 보낼 때이기도 하다(ACT-07.02).
     */
    public void onUplink(long organizationId, long deviceId, long fCnt, Map<String, Map<String, Object>> state) {
        sink.reported(organizationId, new net.java21.data2flow.contracts.message.event.DeviceStateReported(deviceId, Math.max(1, fCnt),
                state, clock.instant(), false));
    }

    @Override
    public Optional<ReportedState> getState(DriverDevice device) {
        return Optional.empty();   // 상태는 업링크(ingress → pipeline)로 들어온다
    }

    @Override
    public void subscribeState(DriverDevice device, StateListener listener) {
        // push 없음: 업링크는 플랫폼 수집 경로로 들어온다
    }

    private static DriverResult accepted(Queued q) {
        return new DriverResult(DriverResult.Status.ACCEPTED, null, false, Map.of("queueItemId", q.queueItemId(), "confirmed", q.confirmed()));
    }

    private String baseUrl(DriverConfig config) {
        String url = config.string("chirpstackUrl", null);
        ChirpStackHostGuard.requireAllowed(url, deniedHosts);
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private HttpResponse<String> send(DriverConfig config, HttpRequest.Builder b) throws IOException, InterruptedException {
        Secret token = config.secret("apiToken");
        if (token != null) {
            b.header("Grpc-Metadata-Authorization", "Bearer " + token.reveal());
        }
        return http.send(b.timeout(timeout).build(), HttpResponse.BodyHandlers.ofString());
    }
}
