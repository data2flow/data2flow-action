package net.java21.data2flow.action.actuation.driver.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import net.java21.data2flow.action.actuation.driver.DeviceDriver;
import net.java21.data2flow.action.actuation.driver.DriverCommand;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverDevice;
import net.java21.data2flow.action.actuation.driver.DriverEventSink;
import net.java21.data2flow.action.actuation.driver.DriverHealth;
import net.java21.data2flow.action.actuation.driver.DriverResult;
import net.java21.data2flow.action.actuation.driver.ReportedState;
import net.java21.data2flow.action.actuation.driver.StateListener;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 일반 드라이버(ACT-03.02, ACT-api §5.3): {@code devices/{device-key}/command}에 QoS1로 발행하고
 * {@code devices/{device-key}/command/ack}·{@code devices/{device-key}/state}를 받아 표준 이벤트로 바꾼다(BR-ACT-25).
 *
 * <p>⏸ 결정 대기(CLAUDE.md §5, ADR-029): 플랫폼 브로커가 공용 {@code iot-data.java21.net}이라 명령 발행은 사용자 결정 전까지 하지 않는다.
 * 그래서 이 드라이버는 설정 {@code data2flow.action.mqtt.enabled=true}일 때만 빈이 되고(기본 꺼짐), 공용 브로커 주소는
 * {@link MqttBrokerGuard}가 접속 전에 거부한다. 지금은 Testcontainers Mosquitto로만 검증한다.
 *
 * <p>메시지: 명령 {@code {"commandId","capability","command","args","validUntil"}} → ack {@code {"commandId","result":"ACKED|FAILED","reason?"}}
 * → state {@code {"version","capabilities":{…}}}. 같은 commandId는 다시 발행하지 않는다(장비도 commandId로 거른다).
 */
public class MqttDriver implements DeviceDriver {

    public static final String TYPE = "MQTT";
    public static final String DEFAULT_COMMAND_TOPIC = "devices/{device-key}/command";
    public static final String DEFAULT_ACK_TOPIC = "devices/{device-key}/command/ack";
    public static final String DEFAULT_STATE_TOPIC = "devices/{device-key}/state";
    static final Set<String> CAPABILITIES = Set.of("Switch", "Thermostat", "FanSpeed", "Ventilation", "Dimmer", "Lock", "Contact");
    private static final Logger log = LoggerFactory.getLogger(MqttDriver.class);
    private static final int SEEN_LIMIT = 10_000;
    private static final TypeReference<Map<String, Map<String, Object>>> CAPS_TYPE = new TypeReference<>() {
    };

    private final ActionProperties.Mqtt settings;
    private final String clientId;
    private final DriverEventSink sink;
    private final Clock clock;
    private final JsonMapper json = JsonMapper.builder().build();
    private final Map<String, Target> devicesByKey = new ConcurrentHashMap<>();
    private final Map<String, StateListener> listeners = new ConcurrentHashMap<>();
    private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
    private final Set<String> published = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > SEEN_LIMIT;
        }
    }));
    private volatile Mqtt5AsyncClient client;

    private record Target(long deviceId, long organizationId) {
    }

    public MqttDriver(ActionProperties.Mqtt settings, String clientId, DriverEventSink sink, Clock clock) {
        MqttBrokerGuard.requireAllowed(settings.host(), settings.deniedHosts());
        this.settings = settings;
        this.clientId = clientId;
        this.sink = sink;
        this.clock = clock;
    }

    /** 브로커에 접속하고 기본 ack·state 토픽을 구독한다 */
    public synchronized void start() {
        if (client != null) {
            return;
        }
        Mqtt5AsyncClient c = MqttClient.builder().useMqttVersion5().identifier(clientId)
                .serverHost(settings.host()).serverPort(settings.port())
                .automaticReconnectWithDefaultConfig()
                .buildAsync();
        var connect = c.connectWith().cleanStart(true);
        if (settings.username() != null && !settings.username().isBlank()) {
            connect = connect.simpleAuth().username(settings.username())
                    .password(settings.password() == null ? new byte[0] : settings.password().getBytes(StandardCharsets.UTF_8))
                    .applySimpleAuth();
        }
        connect.send().orTimeout(10, TimeUnit.SECONDS).join();
        client = c;
        subscribe(filter(DEFAULT_ACK_TOPIC), true);
        subscribe(filter(DEFAULT_STATE_TOPIC), false);
    }

    /** 접속을 끊는다(graceful shutdown) */
    public synchronized void stop() {
        Mqtt5AsyncClient c = client;
        client = null;
        subscribed.clear();
        if (c != null) {
            try {
                c.disconnect().orTimeout(5, TimeUnit.SECONDS).join();
            } catch (RuntimeException e) {
                log.debug("MQTT 접속 종료 중 오류: {}", e.toString());
            }
        }
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public DriverHealth healthCheck(DriverConfig config) {
        Instant start = clock.instant();
        Mqtt5AsyncClient c = client;
        boolean ok = c != null && c.getState().isConnected();
        long ms = Math.max(0, clock.instant().toEpochMilli() - start.toEpochMilli());
        return ok ? DriverHealth.up(ms, CAPABILITIES) : DriverHealth.down(ms, CAPABILITIES, "UNREACHABLE", "MQTT 브로커에 연결되어 있지 않습니다");
    }

    @Override
    public DriverResult execute(DriverCommand command) {
        String key = command.device().externalId();
        if (key == null || key.isBlank()) {
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, false, "기기 외부 ID(device-key)가 없습니다");
        }
        devicesByKey.put(key, new Target(command.device().deviceId(), command.device().organizationId()));
        String id = command.commandId().toString();
        if (published.contains(id)) {
            return DriverResult.accepted();
        }
        Mqtt5AsyncClient c = client;
        if (c == null || !c.getState().isConnected()) {
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, "MQTT 브로커에 연결되어 있지 않습니다");
        }
        DriverConfig cfg = command.device().config();
        subscribe(filter(cfg == null ? DEFAULT_ACK_TOPIC : cfg.string("ackTopic", DEFAULT_ACK_TOPIC)), true);
        String topic = (cfg == null ? DEFAULT_COMMAND_TOPIC : cfg.string("commandTopic", DEFAULT_COMMAND_TOPIC)).replace("{device-key}", key);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("commandId", id);
        body.put("capability", command.capability());
        body.put("command", command.command());
        body.put("args", command.args());
        if (command.validUntil() != null) {
            body.put("validUntil", command.validUntil().toString());
        }
        try {
            c.publishWith().topic(topic).qos(MqttQos.fromCode(settings.qos())).payload(json.writeValueAsBytes(body)).send()
                    .orTimeout(10, TimeUnit.SECONDS).join();
            published.add(id);
            return DriverResult.accepted();
        } catch (RuntimeException e) {
            log.warn("MQTT 명령 발행 실패 topic={} command={}: {}", topic, id, e.toString());
            return DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, e.getMessage());
        }
    }

    /** MQTT는 push 방식이라 폴링 상태가 없다 */
    @Override
    public Optional<ReportedState> getState(DriverDevice device) {
        return Optional.empty();
    }

    @Override
    public void subscribeState(DriverDevice device, StateListener listener) {
        if (device.externalId() == null) {
            return;
        }
        devicesByKey.put(device.externalId(), new Target(device.deviceId(), device.organizationId()));
        if (listener != null) {
            listeners.put(device.externalId(), listener);
        }
        DriverConfig cfg = device.config();
        subscribe(filter(cfg == null ? DEFAULT_STATE_TOPIC : cfg.string("stateTopic", DEFAULT_STATE_TOPIC)), false);
    }

    private void subscribe(String filter, boolean ack) {
        Mqtt5AsyncClient c = client;
        if (c == null || !subscribed.add(filter)) {
            return;
        }
        c.subscribeWith().topicFilter(filter).qos(MqttQos.AT_LEAST_ONCE)
                .callback(p -> onMessage(p, ack))
                .send().orTimeout(10, TimeUnit.SECONDS).join();
    }

    private static String filter(String template) {
        return template.replace("{device-key}", "+");
    }

    void onMessage(Mqtt5Publish publish, boolean ack) {
        String topic = publish.getTopic().toString();
        String[] levels = topic.split("/");
        String key = levels.length > 1 ? levels[1] : null;
        Target target = key == null ? null : devicesByKey.get(key);
        if (target == null) {
            log.warn("모르는 기기의 MQTT {} 메시지를 무시합니다: {}", ack ? "ack" : "state", topic);
            return;
        }
        try {
            JsonNode body = json.readTree(publish.getPayloadAsBytes());
            Instant now = clock.instant();
            if (ack) {
                String result = body.path("result").asString("FAILED");
                DeviceCommandAck a = "ACKED".equals(result)
                        ? DeviceCommandAck.acked(body.path("commandId").asString(), target.deviceId(), now, false)
                        : DeviceCommandAck.failed(body.path("commandId").asString(), target.deviceId(),
                                body.path("reason").asString(CommandStatusReasons.DRIVER_ERROR), now, false);
                sink.ack(target.organizationId(), a);
            } else {
                Map<String, Map<String, Object>> caps = json.convertValue(body.path("capabilities"), CAPS_TYPE);
                long version = body.path("version").asLong(0);
                DeviceStateReported state = new DeviceStateReported(target.deviceId(), version, caps, now, false);
                sink.reported(target.organizationId(), state);
                StateListener l = listeners.get(key);
                if (l != null) {
                    l.onState(target.deviceId(), new ReportedState(key, caps, version, now));
                }
            }
        } catch (RuntimeException e) {
            log.warn("MQTT {} 메시지를 읽을 수 없습니다 topic={}: {}", ack ? "ack" : "state", topic, e.toString());
        }
    }

    /** 접속 상태(시험·지표) */
    public boolean connected() {
        Mqtt5AsyncClient c = client;
        return c != null && c.getState().isConnected();
    }
}
