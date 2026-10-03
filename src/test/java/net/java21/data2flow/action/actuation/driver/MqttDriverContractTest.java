package net.java21.data2flow.action.actuation.driver;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import net.java21.data2flow.action.actuation.driver.mqtt.MqttDriver;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.support.MutableClock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 계약 키트를 MQTT 일반 드라이버로(ACT-03.02, TC-ACT-070). 브로커는 Testcontainers Mosquitto뿐이다(공용 브로커 접속 금지, CLAUDE.md §5).
 * 장비는 시험 안의 MQTT 클라이언트가 흉내 낸다: {@code devices/{key}/command}를 받아 {@code /command/ack}로 답하고 {@code /state}를 낸다.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MqttDriverContractTest extends DriverContractTest {

    @SuppressWarnings("resource")
    private static final GenericContainer<?> MOSQUITTO = new GenericContainer<>("eclipse-mosquitto:2.0")
            .withExposedPorts(1883)
            .withCopyToContainer(Transferable.of("listener 1883\nallow_anonymous true\n"), "/mosquitto/config/mosquitto.conf")
            .waitingFor(Wait.forListeningPort());
    private static final RecordingSink SINK = new RecordingSink();
    private static final Map<String, Integer> EFFECTS = new ConcurrentHashMap<>();
    private static final AtomicLong VERSION = new AtomicLong();
    private static volatile boolean respond = true;
    private static final MqttDriver DRIVER;
    private static final Mqtt5AsyncClient DEVICE_CLIENT;
    private static final DriverDevice DEVICE = new DriverDevice(31, 1, "ws-31", false,
            new DriverConfig(5L, "MQTT", Map.of("commandTopic", MqttDriver.DEFAULT_COMMAND_TOPIC, "ackTopic", MqttDriver.DEFAULT_ACK_TOPIC)));

    static {
        MOSQUITTO.start();
        ActionProperties.Mqtt settings = new ActionProperties.Mqtt(true, MOSQUITTO.getHost(), MOSQUITTO.getMappedPort(1883), null, null,
                null, List.of(), 1);
        DRIVER = new MqttDriver(settings, "data2flow-action-test-" + UUID.randomUUID().toString().substring(0, 8), SINK,
                new MutableClock(MutableClock.T0));
        DRIVER.start();
        DEVICE_CLIENT = MqttClient.builder().useMqttVersion5().identifier("device-ws-31-" + UUID.randomUUID().toString().substring(0, 8))
                .serverHost(MOSQUITTO.getHost()).serverPort(MOSQUITTO.getMappedPort(1883)).buildAsync();
        DEVICE_CLIENT.connect().orTimeout(10, TimeUnit.SECONDS).join();
        DEVICE_CLIENT.subscribeWith().topicFilter("devices/ws-31/command").qos(MqttQos.AT_LEAST_ONCE).callback(p -> {
            JsonNode cmd = Json.MAPPER.readTree(p.getPayloadAsBytes());
            String id = cmd.path("commandId").asString();
            EFFECTS.merge(id, 1, Integer::sum);
            if (respond) {
                DEVICE_CLIENT.publishWith().topic("devices/ws-31/command/ack").qos(MqttQos.AT_LEAST_ONCE)
                        .payload(Json.write(Map.of("commandId", id, "result", "ACKED")).getBytes(StandardCharsets.UTF_8)).send();
            }
        }).send().orTimeout(10, TimeUnit.SECONDS).join();
    }

    @AfterAll
    static void stop() {
        DRIVER.stop();
        DEVICE_CLIENT.disconnect();
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
            public void respond(boolean r) {
                respond = r;
            }

            @Override
            public int effects(UUID commandId) {
                return EFFECTS.getOrDefault(commandId.toString(), 0);
            }

            @Override
            public void emitState(Map<String, Map<String, Object>> capabilities) {
                DEVICE_CLIENT.publishWith().topic("devices/ws-31/state").qos(MqttQos.AT_LEAST_ONCE)
                        .payload(Json.write(Map.of("version", VERSION.incrementAndGet(), "capabilities", capabilities))
                                .getBytes(StandardCharsets.UTF_8)).send().join();
            }

            @Override
            public void breakConnection() {
                DRIVER.stop();
            }
        };
    }

    @Test
    @Order(Integer.MAX_VALUE)
    @DisplayName("[ACT-03.02][AT-ACT-07.4][TC-ACT-070] 브로커 연결이 끊기면 예외 대신 FAILED(DRIVER_ERROR, 재시도 대상)")
    void brokerDownIsRetryableFailure() {
        assertErrorsAreFailedResults();
        assertThat(DRIVER.execute(command(UUID.randomUUID(), Map.of("on", true))).retryable()).isTrue();
        assertThat(DRIVER.healthCheck(DEVICE.config()).ok()).isFalse();
        assertThat(DRIVER.connected()).isFalse();
    }

    @Test
    @DisplayName("[ACT-03.02][TC-ACT-070] 명령 메시지 모양 {commandId, capability, command, args, validUntil}을 QoS1로 발행, 모르는 기기 메시지는 무시")
    void messageShape() {
        Map<String, JsonNode> seen = new ConcurrentHashMap<>();
        Mqtt5AsyncClient spy = MqttClient.builder().useMqttVersion5().identifier("spy-" + UUID.randomUUID().toString().substring(0, 8))
                .serverHost(MOSQUITTO.getHost()).serverPort(MOSQUITTO.getMappedPort(1883)).buildAsync();
        spy.connect().join();
        spy.subscribeWith().topicFilter("devices/+/command").qos(MqttQos.AT_LEAST_ONCE)
                .callback(p -> seen.put(p.getTopic().toString(), Json.MAPPER.readTree(p.getPayloadAsBytes()))).send().join();
        UUID id = UUID.randomUUID();

        DRIVER.execute(command(id, Map.of("on", true)));

        org.awaitility.Awaitility.await().until(() -> seen.containsKey("devices/ws-31/command"));
        JsonNode body = seen.get("devices/ws-31/command");
        assertThat(body.path("commandId").asString()).isEqualTo(id.toString());
        assertThat(body.path("capability").asString()).isEqualTo("Switch");
        assertThat(body.path("args").path("on").asBoolean()).isTrue();
        assertThat(body.path("validUntil").asString()).isEqualTo("2030-01-01T00:00:00Z");
        spy.publishWith().topic("devices/unknown/command/ack").payload("{}".getBytes(StandardCharsets.UTF_8)).send().join();
        spy.disconnect();
        DriverResult noKey = DRIVER.execute(new DriverCommand(UUID.randomUUID(),
                new DriverDevice(32, 1, null, false, DEVICE.config()), "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(noKey.status()).isEqualTo(DriverResult.Status.FAILED);
    }
}
