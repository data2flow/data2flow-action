package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.lorawan.LoRaWanDriver;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.contracts.secret.Secret;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 계약 키트를 LoRaWAN 드라이버로(ACT-03.03, TC-ACT-072). MockWebServer가 ChirpStack v4 REST API를 흉내 낸다. 실제 ChirpStack(공용 s3 포함)은
 * 부르지 않는다(CLAUDE.md §5).
 */
class LoRaWanDriverContractTest extends DriverContractTest {

    private static final String TOKEN = "test-chirpstack-token";
    private static final FakeChirpStack CS = new FakeChirpStack();
    private static final RecordingSink SINK = new RecordingSink();
    private static final MutableClock CLOCK = new MutableClock(MutableClock.T0);
    private static final LoRaWanDriver DRIVER = new LoRaWanDriver(Duration.ofSeconds(2), List.of(), SINK, CLOCK);
    private static final DriverConfig CONFIG = new DriverConfig(11L, "LORAWAN",
            Map.of("chirpstackUrl", CS.url(), "applicationId", "app-1", "confirmed", true), Map.of("apiToken", Secret.of(TOKEN)));
    private static final DriverDevice DEVICE = new DriverDevice(15, 1, "70b3d57ed0000001", false, CONFIG);

    /** ChirpStack v4 API 목 */
    static final class FakeChirpStack extends Dispatcher {
        final MockWebServer server = new MockWebServer();
        final List<JsonNode> queued = new CopyOnWriteArrayList<>();
        final Map<String, AtomicInteger> perCommand = new ConcurrentHashMap<>();
        volatile String mode = "RESPOND";
        final AtomicInteger seq = new AtomicInteger();

        FakeChirpStack() {
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
            if (!("Bearer " + TOKEN).equals(r.getHeader("Grpc-Metadata-Authorization"))) {
                return new MockResponse().setResponseCode(401).setBody("{\"error\":\"authentication failed\"}");
            }
            String path = r.getPath() == null ? "" : r.getPath();
            if (path.startsWith("/api/applications/")) {
                return new MockResponse().setResponseCode(path.endsWith("/app-1") ? 200 : 404).setBody("{\"application\":{\"id\":\"app-1\"}}");
            }
            if (path.matches("/api/devices/[0-9a-f]{16}/queue") && "POST".equals(r.getMethod())) {
                if ("ERROR".equals(mode)) {
                    return new MockResponse().setResponseCode(503);
                }
                JsonNode body = Json.MAPPER.readTree(r.getBody().readUtf8());
                queued.add(body);
                String commandId = r.getHeader("X-Request-Id");
                perCommand.computeIfAbsent(commandId, k -> new AtomicInteger()).incrementAndGet();
                String id = "q-" + seq.incrementAndGet();
                if ("RESPOND".equals(mode)) {
                    // 장비가 다음 수신 창에서 받고 확인(confirmed) → ChirpStack ack 이벤트
                    // EVT-ACT-09(ingress) → action이 DB 큐 항목 ID로 찾은 명령 → 드라이버 정규화 → 표준 ack(BR-ACT-25)
                    CompletableFuture.runAsync(() -> SINK.ack(1, LoRaWanDriver.commandAck(UUID.fromString(commandId), 15,
                                    LoRaWanDownlinkAck.ack(1, "70b3d57ed0000001", id, true, 1L, CLOCK.instant()), true).orElseThrow()),
                            CompletableFuture.delayedExecutor(200, TimeUnit.MILLISECONDS));
                }
                return new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody("{\"id\":\"" + id + "\"}");
            }
            return new MockResponse().setResponseCode(404);
        }
    }

    @Override
    protected DeviceDriver driver() {
        return DRIVER;
    }

    @Override
    protected DriverDevice device() {
        return DEVICE;
    }

    @Override
    protected RecordingSink sink() {
        return SINK;
    }

    @Override
    protected DevicePeer peer() {
        return new DevicePeer() {
            @Override
            public void respond(boolean respond) {
                CS.mode = respond ? "RESPOND" : "SILENT";
            }

            @Override
            public int effects(UUID commandId) {
                AtomicInteger n = CS.perCommand.get(commandId.toString());
                return n == null ? 0 : n.get();
            }

            @Override
            public void emitState(Map<String, Map<String, Object>> capabilities) {
                DRIVER.onUplink(1, 15, 42, capabilities);   // ChirpStack 업링크(디코드된 상태) → 표준 상태 보고
            }

            @Override
            public void breakConnection() {
                CS.mode = "ERROR";
            }
        };
    }

    @Test
    @DisplayName("[ACT-03.03][AT-ACT-11.2][TC-ACT-072] 큐 등록 본문의 fPort·base64 payload가 인코더 출력과 같고 API 키 헤더 포함, 큐 ID·confirmed 반환, EVT-ACT-09 정규화(ACK·미확인·TXACK) → ACKED·FAILED")
    void queueBodyAndAck() {
        UUID id = UUID.randomUUID();
        DriverResult r = DRIVER.execute(new DriverCommand(id, DEVICE, "Thermostat", "set", Map.of("mode", "cool", "targetTemperature", 24),
                null, "k", 1));

        assertThat(r.status()).isEqualTo(DriverResult.Status.ACCEPTED);
        assertThat(r.detail().get("queueItemId")).asString().startsWith("q-");
        JsonNode item = CS.queued.get(CS.queued.size() - 1).path("queueItem");
        assertThat(item.path("fPort").asInt()).isEqualTo(11);
        assertThat(HexFormat.of().formatHex(Base64.getDecoder().decode(item.path("data").asString()))).isEqualTo("020130");
        assertThat(item.path("confirmed").asBoolean()).isTrue();
        await().atMost(Duration.ofSeconds(5)).until(() -> SINK.acks.stream().anyMatch(a -> a.commandId().equals(id.toString())));
        assertThat(r.detail().get("confirmed")).isEqualTo(true);

        // 다운링크 결과 정규화: ACK 확인 → ACKED, 미확인 → FAILED, 확인형의 TXACK는 기다림, 비확인형의 TXACK → ACKED
        String q = (String) r.detail().get("queueItemId");
        assertThat(LoRaWanDriver.commandAck(id, 15, LoRaWanDownlinkAck.ack(1, "70b3d57ed0000001", q, false, 2L, CLOCK.instant()), true)
                .orElseThrow()).satisfies(a -> {
                    assertThat(a.result()).isEqualTo(DeviceCommandAck.Result.FAILED);
                    assertThat(a.reason()).isEqualTo(LoRaWanDriver.NOT_ACKNOWLEDGED);
                    assertThat(a.commandId()).isEqualTo(id.toString());
                });
        assertThat(LoRaWanDriver.commandAck(id, 15, LoRaWanDownlinkAck.txAck(1, "70b3d57ed0000001", q, 2L, CLOCK.instant()), true)).isEmpty();
        assertThat(LoRaWanDriver.commandAck(id, 15, LoRaWanDownlinkAck.txAck(1, "70b3d57ed0000001", q, 2L, CLOCK.instant()), false)
                .orElseThrow().result()).isEqualTo(DeviceCommandAck.Result.ACKED);
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] 401 → 연결 확인 실패(AUTH), 오류는 예외 대신 FAILED(reason), 지원하지 않는 기능은 재시도 없음")
    void errors() {
        DriverConfig wrong = new DriverConfig(11L, "LORAWAN", CONFIG.config(), Map.of("apiToken", Secret.of("wrong")));
        DriverHealth h = DRIVER.healthCheck(wrong);
        assertThat(h.ok()).isFalse();
        assertThat(h.errorKind()).isEqualTo("AUTH");
        DriverResult auth = DRIVER.execute(new DriverCommand(UUID.randomUUID(), new DriverDevice(15, 1, "70b3d57ed0000001", false, wrong),
                "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(auth.reason()).isEqualTo("AUTH");
        assertThat(auth.retryable()).isFalse();
        DriverResult contact = DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Contact", "set", Map.of(), null, "k", 1));
        assertThat(contact.reason()).isEqualTo("CAPABILITY_NOT_SUPPORTED");
        DriverResult noEui = DRIVER.execute(new DriverCommand(UUID.randomUUID(), new DriverDevice(15, 1, null, false, CONFIG), "Switch", "set",
                Map.of("on", true), null, "k", 1));
        assertThat(noEui.reason()).isEqualTo("INVALID_COMMAND");
        assertThat(DRIVER.healthCheck(new DriverConfig(11L, "LORAWAN", Map.of("chirpstackUrl", CS.url()), CONFIG.secrets())).errorKind())
                .isEqualTo("CONFIG");
        assertThat(DRIVER.healthCheck(new DriverConfig(11L, "LORAWAN", Map.of("chirpstackUrl", CS.url(), "applicationId", "nope"),
                CONFIG.secrets())).errorKind()).isEqualTo("NOT_FOUND");
        assertThat(DRIVER.healthCheck(new DriverConfig(11L, "LORAWAN", Map.of("chirpstackUrl", "http://localhost:1", "applicationId", "a"),
                CONFIG.secrets())).errorKind()).isEqualTo("UNREACHABLE");
        assertThat(DRIVER.getState(DEVICE)).isEmpty();
        assertErrorsAreFailedResults();
        assertThat(DRIVER.execute(command(UUID.randomUUID(), Map.of("on", true))).retryable()).isTrue();
    }

    @Test
    @DisplayName("[ACT-03.03] 안전: 공용 ChirpStack(s3.java21.net)·공용 브로커 주소는 연결 확인·다운링크 모두 거부(CLAUDE.md §5)")
    void sharedChirpStackRefused() {
        DriverConfig shared = new DriverConfig(11L, "LORAWAN", Map.of("chirpstackUrl", "https://s3.java21.net:8080", "applicationId", "a"),
                CONFIG.secrets());
        assertThat(DRIVER.healthCheck(shared).ok()).isFalse();
        DriverResult r = DRIVER.execute(new DriverCommand(UUID.randomUUID(), new DriverDevice(15, 1, "70b3d57ed0000001", false, shared),
                "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(r.status()).isEqualTo(DriverResult.Status.FAILED);
        assertThat(r.reason()).isEqualTo("DRIVER_UNAVAILABLE");
        assertThat(r.retryable()).isFalse();
    }
}
