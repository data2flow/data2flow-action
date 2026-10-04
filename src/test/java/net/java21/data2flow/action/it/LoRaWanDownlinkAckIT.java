package net.java21.data2flow.action.it;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.contracts.secret.Secret;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * ACT-03.03 TC-ACT-072(action 부분, ADR-054 남은 것 ①): LoRaWAN 드라이버(ChirpStack v4 REST 목, MockWebServer)로 Class A 다운링크를 다음 업링크 직후 등록하면 큐 항목 ID가
 * {@code commands.downlink_queue_item_id}에 남고, ingress가 내는 EVT-ACT-09 {@code lorawan.downlink.ack}(RabbitMQ {@code action.events})로
 * 그 명령을 찾아 SENT → ACKED/FAILED 한다. 같은 이벤트를 다시 받아도 한 번만 바뀐다. 실제 ChirpStack(공용 s3 포함)은 부르지 않는다(CLAUDE.md §5).
 */
@TestPropertySource(properties = "data2flow.action.lorawan.enabled=true")
class LoRaWanDownlinkAckIT extends IntegrationTestSupport {

    static final long VALVE = 32_001;
    static final String DEV_EUI = "70b3d57ed0000002";
    static final ChirpStackApi CS = new ChirpStackApi();

    @Autowired
    MeterRegistry meters;
    RestClient operator;
    long fCnt = 100;

    /** ChirpStack v4 {@code POST /api/devices/{dev-eui}/queue} 목: 큐 항목 ID(UUID)를 돌려준다 */
    static final class ChirpStackApi extends Dispatcher {
        final MockWebServer server = new MockWebServer();
        final List<String> queueIds = new CopyOnWriteArrayList<>();

        ChirpStackApi() {
            server.setDispatcher(this);
            try {
                server.start();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public MockResponse dispatch(RecordedRequest r) {
            if (r.getPath() != null && r.getPath().equals("/api/devices/" + DEV_EUI + "/queue") && "POST".equals(r.getMethod())) {
                String id = UUID.randomUUID().toString();
                queueIds.add(id);
                return new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody("{\"id\":\"" + id + "\"}");
            }
            return new MockResponse().setResponseCode(404);
        }
    }

    @AfterAll
    static void stop() throws IOException {
        CS.server.shutdown();
    }

    @BeforeEach
    void setUp() {
        valve(true);
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        operator = api(Fixtures.USER, Fixtures.ORG);
    }

    private void valve(boolean confirmed) {
        DriverBinding lorawan = new DriverBinding(21L, "LORAWAN", Map.of("chirpstackUrl", "http://localhost:" + CS.server.getPort(),
                "applicationId", "app-1", "confirmed", confirmed), 30, 60, null, null, Map.of("apiToken", Secret.of("test-token")));
        CORE.profiles.put(VALVE, new ControlProfile(VALVE, Fixtures.ORG, Fixtures.SPACE, "LoRaWAN 밸브", DEV_EUI, false, "ACTIVE", 5L,
                Map.of("Switch", new ModelCapability(Map.of(), null, false, true)), lorawan, Fixtures.settings()));
        profiles.invalidateAll();
    }

    /** 명령을 보내 SENT가 될 때까지 기다리고, 저장된 큐 항목 ID를 돌려준다 */
    private String[] sendDownlink(String key) {
        Result r = post(operator, "/internal/action/commands", Map.of("deviceId", Long.toString(VALVE), "capability", "Switch",
                "command", "set", "args", Map.of("on", true)), key);
        String id = r.response().path("id").asString();
        // LoRaWAN 기기는 Class A: 다음 업링크(pipeline 업링크 신호 EVT-ACT-07, 상태 없음) 직후에 드라이버로 보낸다(ACT-07.02)
        assertThat(status(id)).isEqualTo("QUEUED_FOR_DOWNLINK");
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(VALVE, ++fCnt, Map.of(), clock.instant(), false));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(jdbc.sql(
                "SELECT status || ':' || COALESCE(status_reason, '') FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)")
                .param("id", id).query(String.class).single()).as(r.response().toString()).isEqualTo("SENT:"));
        String queueItemId = jdbc.sql("SELECT downlink_queue_item_id FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)")
                .param("id", id).query(String.class).single();
        return new String[]{id, queueItemId};
    }

    private long transitions(String id) {
        return count("SELECT count(*) FROM data2flow_action.command_events WHERE command_id = '" + id + "'");
    }

    private double acks(String result) {
        return meters.counter("data2flow_action_downlink_acks_total", "result", result).count();
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] 다운링크 등록 → 큐 항목 ID를 DB에 남기고, EVT-ACT-09 ACK(확인) → ACKED, 같은 이벤트 재전달은 한 번만")
    void ackedAndIdempotent() {
        String[] sent = sendDownlink("dl-1");
        assertThat(sent[1]).isEqualTo(CS.queueIds.getLast());
        assertThat(jdbc.sql("SELECT driver_response ->> 'confirmed' FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)")
                .param("id", sent[0]).query(String.class).single()).isEqualTo("true");

        // 확인형 다운링크의 게이트웨이 송신(TXACK)은 기기 확인이 아니므로 SENT 그대로
        double ignored = acks("ignored");
        publish(EventType.LORAWAN_DOWNLINK_ACK, LoRaWanDownlinkAck.txAck(1, DEV_EUI, sent[1], 7L, clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> acks("ignored") > ignored);
        assertThat(status(sent[0])).isEqualTo("SENT");

        LoRaWanDownlinkAck ack = LoRaWanDownlinkAck.ack(1, DEV_EUI, sent[1], true, 7L, clock.instant());
        publish(EventType.LORAWAN_DOWNLINK_ACK, ack);
        await().atMost(Duration.ofSeconds(20)).until(() -> status(sent[0]).equals("ACKED"));
        long after = transitions(sent[0]);
        double acked = acks("ACKED");

        publish(EventType.LORAWAN_DOWNLINK_ACK, ack);   // 이중 ingress·재전달
        await().atMost(Duration.ofSeconds(20)).until(() -> acks("ACKED") > acked);
        assertThat(status(sent[0])).isEqualTo("ACKED");
        assertThat(transitions(sent[0])).isEqualTo(after);
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] EVT-ACT-09 ACK(acknowledged=false) → FAILED(DOWNLINK_NOT_ACKNOWLEDGED)")
    void notAcknowledged() {
        String[] sent = sendDownlink("dl-2");
        publish(EventType.LORAWAN_DOWNLINK_ACK, LoRaWanDownlinkAck.ack(1, DEV_EUI, sent[1], false, 8L, clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> status(sent[0]).equals("FAILED"));
        assertThat(jdbc.sql("SELECT status_reason FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)")
                .param("id", sent[0]).query(String.class).single()).isEqualTo("DOWNLINK_NOT_ACKNOWLEDGED");
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] 비확인형(confirmed=false) 다운링크는 게이트웨이 송신(TXACK)으로 ACKED")
    void unconfirmedTxAck() {
        valve(false);
        String[] sent = sendDownlink("dl-3");
        publish(EventType.LORAWAN_DOWNLINK_ACK, LoRaWanDownlinkAck.txAck(1, DEV_EUI, sent[1], 9L, clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> status(sent[0]).equals("ACKED"));
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-072] 메모리가 아니라 DB로 찾는다: 다른 파드가 등록한 명령도 ACKED, 다른 조직·모르는 큐 항목은 무시")
    void lookupIsByDatabaseAndOrganization() {
        String id = UUID.randomUUID().toString();
        String queueItemId = UUID.randomUUID().toString();
        jdbc.sql("""
                INSERT INTO data2flow_action.commands (id, organization_id, idempotency_key, device_id, capability, command, args, priority,
                    source, status, valid_until, attempts, requested_at, sent_at, timeout_at, driver_response, downlink_queue_item_id)
                VALUES (CAST(:id AS uuid), 1, repeat('a', 64), :device, 'Switch', 'set', '{"on":true}', 'MANUAL',
                    '{"type":"USER","userId":"7"}', 'SENT', :validUntil, 1, :now, :now, :timeout,
                    CAST(:resp AS jsonb), :q)""")
                .param("id", id).param("device", VALVE).param("now", java.sql.Timestamp.from(clock.instant()))
                .param("validUntil", java.sql.Timestamp.from(clock.instant().plusSeconds(600)))
                .param("timeout", java.sql.Timestamp.from(clock.instant().plusSeconds(30)))
                .param("resp", "{\"queueItemId\":\"" + queueItemId + "\",\"confirmed\":true}").param("q", queueItemId)
                .update();

        double unknown = acks("unknown");
        publish(EventType.LORAWAN_DOWNLINK_ACK, Fixtures.OTHER_ORG, LoRaWanDownlinkAck.ack(1, DEV_EUI, queueItemId, true, 1L, clock.instant()));
        publish(EventType.LORAWAN_DOWNLINK_ACK, LoRaWanDownlinkAck.ack(1, DEV_EUI, UUID.randomUUID().toString(), true, 1L, clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> acks("unknown") >= unknown + 2);
        assertThat(status(id)).isEqualTo("SENT");

        publish(EventType.LORAWAN_DOWNLINK_ACK, LoRaWanDownlinkAck.ack(1, DEV_EUI, queueItemId, true, 1L, clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("ACKED"));
    }
}
