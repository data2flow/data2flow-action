package net.java21.data2flow.action.output;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.repository.OutputDeliveryRepository;
import net.java21.data2flow.action.output.service.OutputDeliveryService;
import net.java21.data2flow.action.output.service.OutputEnqueueService;
import net.java21.data2flow.action.output.service.OutputStats;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 출력 연결 발송(DSC-04.01, BR-DSC-19): 필터, 연결별 순서 발송, 장애 뒤 밀린 메시지, 24시간 뒤 실패 보관함과 재전송, 멱등 쌓기,
 * 연결 테스트(API-DSC-76)·재전송(API-DSC-77). 외부 대상은 Testcontainers Mosquitto와 MockWebServer(Webhook)다.
 */
class OutputDeliveryIT extends OutputSupport {

    @Autowired
    OutputEnqueueService enqueue;
    @Autowired
    OutputDeliveryService delivery;
    @Autowired
    OutputDeliveryRepository repository;
    @Autowired
    OutputStats stats;

    private MockWebServer hook;
    private final List<RecordedRequest> hookRequests = new CopyOnWriteArrayList<>();
    private volatile int hookStatus = 200;

    @BeforeEach
    void startHook() throws IOException {
        hook = new MockWebServer();
        hook.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                hookRequests.add(request);
                return new MockResponse().setResponseCode(hookStatus).setBody(hookStatus < 300 ? "{\"ok\":true}" : "{\"error\":\"down\"}");
            }
        });
        hook.start();
    }

    @AfterEach
    void stopHook() throws IOException {
        hook.shutdown();
    }

    private String hookUrl() {
        return hook.url("/ingest").toString();
    }

    private Map<String, Object> webhook(long id) {
        return OutputFixtures.webhook(id, hookUrl(), Map.of(), Map.of(), Map.of());
    }

    private String status(long rowsOf, String status) {
        return Long.toString(count("SELECT count(*) FROM data2flow_action.output_deliveries WHERE output_id = " + rowsOf
                + " AND status = '" + status + "'"));
    }

    @Test
    @DisplayName("[DSC-04.01][AT-DSC-10.1][TC-DSC-126] 필터 metrics=[co2]: 실습실 AM107 수신 → 외부 MQTT(Mosquitto)에 co2만 d2f/R101/am107-lab/co2로 전달, 1분 지표 전송")
    void co2OnlyToMqtt() throws Exception {
        setConnections(List.of(OutputFixtures.mqtt(11, mosquittoUrl(), "d2f/{spaceCode}/{deviceName}/{metric}", Map.of("metrics", List.of("co2")))));
        List<String> topics = new CopyOnWriteArrayList<>();
        List<String> bodies = new CopyOnWriteArrayList<>();
        Mqtt5AsyncClient sub = MqttClient.builder().useMqttVersion5().identifier("data2flow-action-test-sub-" + UUID.randomUUID())
                .serverHost(MOSQUITTO.getHost()).serverPort(MOSQUITTO.getMappedPort(1883)).buildAsync();
        sub.connect().get(10, TimeUnit.SECONDS);
        sub.subscribeWith().topicFilter("d2f/#").qos(MqttQos.AT_LEAST_ONCE).callback(p -> {
            topics.add(p.getTopic().toString());
            bodies.add(StandardCharsets.UTF_8.decode(p.getPayload().orElseThrow()).toString());
        }).send().get(10, TimeUnit.SECONDS);
        try {
            assertThat(enqueue.enqueue(List.of(OutputFixtures.am107(clock.instant(), 812)))).isEqualTo(1);
            assertThat(delivery.deliverDue()).isEqualTo(1);
            await().atMost(Duration.ofSeconds(10)).until(() -> topics.size() == 1);
            assertThat(topics).containsExactly("d2f/R101/am107-lab/co2");
            CanonicalTelemetry sent = Json.read(bodies.getFirst(), CanonicalTelemetry.class);
            assertThat(sent.metrics()).extracting(CanonicalTelemetry.Metric::key).containsExactly("co2");
            assertThat(status(11, "SENT")).isEqualTo("1");
        } finally {
            sub.disconnect().get(5, TimeUnit.SECONDS);
        }
        assertThat(stats.flush(true)).isEqualTo(1);
        assertThat(statsItems).singleElement().satisfies(i -> {
            assertThat(i.path("outputId").asString()).isEqualTo("11");
            assertThat(i.path("sent").asInt()).isEqualTo(1);
            assertThat(i.path("failed").asInt()).isZero();
        });
    }

    @Test
    @DisplayName("[DSC-04.01][AT-DSC-10.2][BR-DSC-19][TC-DSC-126] 대상 1시간 장애: 쌓기(수집 쪽)는 계속되고, 복구되면 밀린 메시지를 순서대로 전달, 재시도는 지수 백오프")
    void outageThenOrderedDelivery() {
        setConnections(List.of(webhook(21)));
        hookStatus = 503;
        List<Instant> produced = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            produced.add(clock.instant());
            assertThat(enqueue.enqueue(List.of(OutputFixtures.am107(clock.instant(), 600 + i)))).isEqualTo(1);
            clock.advanceBy(Duration.ofSeconds(1));
        }
        assertThat(delivery.deliverDue()).isZero();
        Instant end = clock.instant().plus(Duration.ofHours(1));
        int minute = 0;
        while (clock.instant().isBefore(end)) {
            clock.advanceBy(Duration.ofSeconds(30));
            if (++minute % 20 == 0) {
                // 장애 중에도 새 텔레메트리는 바로 쌓인다(별도 소비자 → 수집 지연 없음)
                produced.add(clock.instant());
                assertThat(enqueue.enqueue(List.of(OutputFixtures.am107(clock.instant(), 700 + minute)))).isEqualTo(1);
            }
            delivery.deliverDue();
        }
        int attemptsDuringOutage = hookRequests.size();
        assertThat(attemptsDuringOutage).as("1초부터 2배·최대 5분 간격이라 1시간에 수십 번 이하").isBetween(5, 40);
        assertThat(status(21, "PENDING")).isEqualTo(Integer.toString(produced.size()));

        hookStatus = 200;
        clock.advanceBy(Duration.ofMinutes(5));
        while (delivery.deliverDue() > 0) {
            // 밀린 것을 모두 보낸다
        }
        assertThat(status(21, "SENT")).isEqualTo(Integer.toString(produced.size()));
        List<Instant> deliveredOrder = new ArrayList<>();
        for (RecordedRequest r : hookRequests.subList(attemptsDuringOutage, hookRequests.size())) {
            Json.MAPPER.readTree(r.getBody().readUtf8()).forEach(t -> deliveredOrder.add(Instant.parse(t.path("measuredAt").asString())));
        }
        assertThat(deliveredOrder).containsExactlyElementsOf(produced);
        stats.flush(true);
        assertThat(statsItems.stream().mapToInt(i -> i.path("failed").asInt()).sum()).isGreaterThanOrEqualTo(attemptsDuringOutage);
        assertThat(statsItems.stream().mapToInt(i -> i.path("sent").asInt()).sum()).isEqualTo(produced.size());
        assertThat(statsItems.stream().mapToInt(i -> i.path("retried").asInt()).sum()).as("장애 중 시도된 행만 재시도로 셈")
                .isBetween(3, produced.size());
    }

    @Test
    @DisplayName("[DSC-04.01][BR-DSC-19] 24시간 넘게 실패하면 실패 보관함(FAILED)으로 옮겨 뒤 메시지를 막지 않고, API-DSC-77 재전송으로 다시 보낸다")
    void failedInboxAndReplay() {
        setConnections(List.of(webhook(31)));
        hookStatus = 500;
        Instant first = clock.instant();
        enqueue.enqueue(List.of(OutputFixtures.am107(first, 900)));
        delivery.deliverDue();
        for (int i = 0; i < 300; i++) {
            clock.advanceBy(Duration.ofMinutes(5));
            delivery.deliverDue();
        }
        assertThat(status(31, "FAILED")).isEqualTo("1");
        assertThat(jdbc.sql("SELECT failure_kind FROM data2flow_action.output_deliveries WHERE output_id = 31").query(String.class).single())
                .isEqualTo("HTTP_STATUS");

        hookStatus = 200;
        enqueue.enqueue(List.of(OutputFixtures.am107(clock.instant(), 901)));
        assertThat(delivery.deliverDue()).as("실패 보관함 행이 뒤를 막지 않음").isEqualTo(1);

        RestClient core = api(1, 1);
        Result bad = post(core, "/internal/action/output-connections/31/replay-failed",
                Map.of("organizationId", "1", "from", first.plusSeconds(10).toString(), "to", first.toString()), null);
        assertThat(bad.status()).isEqualTo(400);
        Result replay = post(core, "/internal/action/output-connections/31/replay-failed",
                Map.of("organizationId", "1", "from", first.minusSeconds(1).toString(), "to", clock.instant().toString()), null);
        assertThat(replay.status()).isEqualTo(200);
        assertThat(replay.response().path("queued").asInt()).isEqualTo(1);
        assertThat(post(core, "/internal/action/output-connections/31/replay-failed",
                Map.of("organizationId", 2, "from", first.minusSeconds(1).toString(), "to", clock.instant().toString()), null)
                .response().path("queued").asInt()).as("다른 조직은 0").isZero();
        assertThat(delivery.deliverDue()).isEqualTo(1);
        assertThat(status(31, "SENT")).isEqualTo("2");
    }

    @Test
    @DisplayName("[DSC-04.01] 쌓기는 멱등(같은 메시지 두 번 → 한 행), 꺼진 연결·필터 밖·공용 브로커 주소 연결은 쌓지 않고, 지워진 연결의 대기 행은 실패 보관함으로")
    void enqueueRules() {
        Map<String, Object> disabled = new LinkedHashMap<>(webhook(42));
        disabled.put("enabled", false);
        setConnections(List.of(webhook(41), disabled,
                OutputFixtures.webhook(43, hookUrl(), Map.of("metrics", List.of("pm10")), Map.of(), Map.of()),
                OutputFixtures.mqtt(44, "wss://iot-data.java21.net:443/mqtt", "d2f/{deviceId}", Map.of())));
        CanonicalTelemetry t = OutputFixtures.am107(clock.instant(), 812);
        assertThat(enqueue.enqueue(List.of(t))).isEqualTo(1);
        assertThat(enqueue.enqueue(List.of(t))).isZero();
        assertThat(count("SELECT count(*) FROM data2flow_action.output_deliveries")).isEqualTo(1);

        CanonicalTelemetry otherOrg = CanonicalTelemetry.builder().messageId(UUID.randomUUID()).organizationId(2).sourceId(1).externalId("x")
                .deviceId(77).deviceStatus(CanonicalTelemetry.DeviceStatus.ACTIVE).measuredAt(clock.instant()).receivedAt(clock.instant())
                .rawMessageId(1).metric(CanonicalTelemetry.Metric.of("co2", 1, "ppm")).build();
        assertThat(enqueue.enqueue(List.of(otherOrg))).as("연결이 없는 조직").isZero();

        setConnections(List.of());
        delivery.deliverDue();
        assertThat(status(41, "FAILED")).isEqualTo("1");
        assertThat(jdbc.sql("SELECT last_error FROM data2flow_action.output_deliveries WHERE output_id = 41").query(String.class).single())
                .isEqualTo("CONNECTION_REMOVED");
        assertThat(hookRequests).isEmpty();

        // 보관 정리(7일)
        assertThat(repository.deleteExpired(clock.instant().plus(Duration.ofDays(8)))).isEqualTo(1);
    }

    @Test
    @DisplayName("[DSC-04.01] 연결마다 리스를 잡은 파드 하나만 보낸다(다른 파드가 잡고 있으면 건너뜀, 리스가 끝나면 넘겨받음)")
    void senderLease() {
        setConnections(List.of(webhook(51)));
        enqueue.enqueue(List.of(OutputFixtures.am107(clock.instant(), 812)));
        assertThat(repository.acquireLease(1, 51, "test", "other-pod", clock.instant(), clock.instant().plusSeconds(60))).isTrue();
        assertThat(delivery.deliverDue()).isZero();
        clock.advanceBy(Duration.ofSeconds(61));
        assertThat(delivery.deliverDue()).isEqualTo(1);
        delivery.releaseLeases();
        assertThat(count("SELECT count(*) FROM data2flow_action.output_sender_leases")).isZero();
    }

    @Test
    @DisplayName("[DSC-04.01][API-DSC-76] Webhook 테스트: 샘플을 실제로 보내고 HMAC 서명(X-D2F-Timestamp·X-D2F-Signature)·인증 헤더·비밀 아닌 헤더를 붙인다")
    void webhookTestSignsRequest() throws Exception {
        Map<String, Object> conn = OutputFixtures.webhook(0, hookUrl(), Map.of("metrics", List.of("co2")),
                Map.of("headers", Map.of("X-Site", "lab", "Host", "evil")), Map.of("HMAC_KEY", "hook-secret", "HEADER_VALUE", "Bearer abc"));
        Result r = post(api(1, 1), "/internal/action/output-connections/test", testBody(conn), null);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.response().path("ok").asBoolean()).isTrue();
        assertThat(r.response().path("response").path("status").asInt()).isEqualTo(200);
        assertThat(r.response().path("response").path("bodyPreview").asString()).contains("ok");
        RecordedRequest req = hookRequests.getFirst();
        String body = req.getBody().readUtf8();
        assertThat(body).isEqualTo(r.response().path("rendered").asString());
        JsonNode arr = Json.MAPPER.readTree(body);
        assertThat(arr.get(0).path("metrics")).hasSize(1);
        String ts = req.getHeader("X-D2F-Timestamp");
        assertThat(ts).isEqualTo(Long.toString(clock.instant().getEpochSecond()));
        assertThat(req.getHeader("X-D2F-Signature")).isEqualTo("v1=" + net.java21.data2flow.action.output.transport.WebhookOutputSender
                .hmac("hook-secret", ts + "." + body));
        assertThat(req.getHeader("Authorization")).isEqualTo("Bearer abc");
        assertThat(req.getHeader("X-Site")).isEqualTo("lab");
        assertThat(req.getHeader("Host")).doesNotContain("evil");
    }

    @Test
    @DisplayName("[DSC-04.01][API-DSC-76] 테스트 실패는 200 + ok=false와 failureKind: 401 AUTH, 500 HTTP_STATUS, 연결 거부 REFUSED, 이름 없음 DNS, 응답 지연 TIMEOUT")
    void webhookTestFailureKinds() throws Exception {
        RestClient core = api(1, 1);
        hookStatus = 401;
        assertThat(kind(core, OutputFixtures.webhook(0, hookUrl(), Map.of(), Map.of(), Map.of()))).isEqualTo("AUTH");
        hookStatus = 500;
        JsonNode r = post(core, "/internal/action/output-connections/test", testBody(OutputFixtures.webhook(0, hookUrl(), Map.of(), Map.of(),
                Map.of())), null).response();
        assertThat(r.path("ok").asBoolean()).isFalse();
        assertThat(r.path("failureKind").asString()).isEqualTo("HTTP_STATUS");
        assertThat(r.path("response").path("status").asInt()).isEqualTo(500);
        int closedPort;
        try (ServerSocket s = new ServerSocket(0)) {
            closedPort = s.getLocalPort();
        }
        assertThat(kind(core, OutputFixtures.webhook(0, "http://127.0.0.1:" + closedPort + "/x", Map.of(), Map.of(), Map.of())))
                .isEqualTo("REFUSED");
        assertThat(kind(core, OutputFixtures.webhook(0, "http://no-such-host.invalid/x", Map.of(), Map.of(), Map.of()))).isEqualTo("DNS");
        MockWebServer slow = new MockWebServer();
        slow.enqueue(new MockResponse().setHeadersDelay(3, TimeUnit.SECONDS));
        slow.start();
        try {
            assertThat(kind(core, OutputFixtures.webhook(0, slow.url("/").toString(), Map.of(), Map.of("timeoutMs", 1000), Map.of())))
                    .isEqualTo("TIMEOUT");
        } finally {
            slow.shutdown();
        }
        Map<String, Object> noOrg = new LinkedHashMap<>(testBody(OutputFixtures.webhook(0, hookUrl(), Map.of(), Map.of(), Map.of())));
        noOrg.remove("organizationId");
        assertThat(post(core, "/internal/action/output-connections/test", noOrg, null).status()).isEqualTo(400);
    }

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("[DSC-04.01][API-DSC-76][CLAUDE.md §5] MQTT 테스트: Mosquitto에 하나 발행, 인증 거부는 AUTH, 공용 브로커 주소는 접속하지 않고 REFUSED(렌더 결과는 보여 줌)")
    void mqttTest() throws Exception {
        RestClient core = api(1, 1);
        AtomicInteger received = new AtomicInteger();
        Mqtt5AsyncClient sub = MqttClient.builder().useMqttVersion5().identifier("data2flow-action-test-sub-" + UUID.randomUUID())
                .serverHost(MOSQUITTO.getHost()).serverPort(MOSQUITTO.getMappedPort(1883)).buildAsync();
        sub.connect().get(10, TimeUnit.SECONDS);
        sub.subscribeWith().topicFilter("lab/#").callback(p -> received.incrementAndGet()).send().get(10, TimeUnit.SECONDS);
        try {
            JsonNode ok = post(core, "/internal/action/output-connections/test",
                    testBody(OutputFixtures.mqtt(0, mosquittoUrl(), "lab/{deviceName}/{metric}", Map.of("metrics", List.of("pm25")))), null)
                    .response();
            assertThat(ok.path("ok").asBoolean()).isTrue();
            assertThat(ok.path("topic").asString()).as("필터가 샘플을 모두 걸러도 샘플 그대로 보낸다").startsWith("lab/am107-lab/");
            await().atMost(Duration.ofSeconds(10)).until(() -> received.get() == 1);
        } finally {
            sub.disconnect().get(5, TimeUnit.SECONDS);
        }
        Map<String, Object> auth = OutputFixtures.mqtt(0, mosquittoAuthUrl(), "lab/{metric}", Map.of());
        auth.put("secrets", Map.of("PASSWORD", "wrong"));
        ((Map<String, Object>) auth.get("target")).put("username", "nobody");
        assertThat(kind(core, auth)).isEqualTo("AUTH");
        JsonNode shared = post(core, "/internal/action/output-connections/test",
                testBody(OutputFixtures.mqtt(0, "wss://iot-data.java21.net/mqtt", "d2f/{metric}", Map.of())), null).response();
        assertThat(shared.path("ok").asBoolean()).isFalse();
        assertThat(shared.path("failureKind").asString()).isEqualTo("REFUSED");
        assertThat(shared.path("rendered").asString()).contains("\"deviceId\":501");
    }

    @Test
    @DisplayName("[DSC-04.01] Webhook TEMPLATE 배치: JSON이 아닌 렌더 결과는 문자열 항목으로, 배치 크기만큼 묶어 보낸다")
    void templateBatch() {
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("url", hookUrl());
        target.put("batchSize", 2);
        target.put("batchWaitMs", 0);
        setConnections(List.of(OutputFixtures.connection(61, "WEBHOOK", target, Map.of("metrics", List.of("co2")), "TEMPLATE",
                "{{deviceName}} co2={{value}}", Map.of())));
        for (int i = 0; i < 3; i++) {
            enqueue.enqueue(List.of(OutputFixtures.am107(clock.instant(), 800 + i)));
        }
        assertThat(delivery.deliverDue()).isEqualTo(3);
        assertThat(hookRequests).hasSize(2);
        JsonNode firstBatch = Json.MAPPER.readTree(hookRequests.getFirst().getBody().readUtf8());
        assertThat(firstBatch).hasSize(2);
        assertThat(firstBatch.get(0).asString()).isEqualTo("am107-lab co2=800");
    }

    private String kind(RestClient core, Map<String, Object> conn) {
        JsonNode r = post(core, "/internal/action/output-connections/test", testBody(conn), null).response();
        assertThat(r.path("ok").asBoolean()).isFalse();
        return r.path("failureKind").asString();
    }

    private Map<String, Object> testBody(Map<String, Object> conn) {
        Map<String, Object> body = new LinkedHashMap<>(conn);
        body.remove("id");
        body.put("sample", Json.MAPPER.valueToTree(OutputFixtures.am107(clock.instant(), 812)));
        body.put("context", OutputFixtures.contextJson());
        return body;
    }
}
