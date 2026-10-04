package net.java21.data2flow.action.output.transport;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import net.java21.data2flow.action.output.domain.DeliveryResult;
import net.java21.data2flow.action.output.domain.HostGuard;
import net.java21.data2flow.action.output.domain.OutboundMessage;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.contracts.output.OutputFailureKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 출력 연결 MQTT 발행(DSC-04.01, type=MQTT_PUBLISH). 주소는 {@code mqtt://}·{@code mqtts://}·{@code ws://}·{@code wss://}, QoS 0·1, 비밀값
 * PASSWORD(사용자 {@code target.username})·CA_CERT. 연결마다 클라이언트 하나를 두고(정의 버전이 바뀌거나 실패하면 다시 만든다),
 * 공용 브로커 {@code iot-data.java21.net}(하위 이름·같은 IP)에는 <b>접속하지 않는다</b>(CLAUDE.md §5, {@link HostGuard}).
 * MQTT 클라이언트(hivemq)는 이 패키지에서만 쓴다(ArchitectureTest).
 */
public class MqttOutputPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MqttOutputPublisher.class);

    private final List<String> deniedHosts;
    private final String clientIdBase;
    private final Duration timeout;
    private final Map<Long, Cached> clients = new ConcurrentHashMap<>();

    private record Cached(long version, Mqtt5AsyncClient client) {
    }

    public MqttOutputPublisher(List<String> deniedHosts, String clientIdBase, Duration timeout) {
        this.deniedHosts = List.copyOf(deniedHosts);
        this.clientIdBase = clientIdBase;
        this.timeout = timeout;
    }

    /** 메시지 하나를 발행한다(연결 캐시 사용). 실패하면 연결을 버려 다음에 다시 접속한다 */
    public DeliveryResult publish(OutputConnection c, OutboundMessage m) {
        if (HostGuard.denied(c.url(), deniedHosts)) {
            log.warn("출력 연결 {}의 MQTT 주소는 접속 금지입니다(CLAUDE.md §5). 보내지 않습니다", c.id());
            return DeliveryResult.failure(OutputFailureKind.REFUSED, "FORBIDDEN_HOST", null, null);
        }
        try {
            Mqtt5AsyncClient client = client(c);
            send(client, c, m);
            return DeliveryResult.success(null, null);
        } catch (Exception e) {
            discard(c.id());
            return DeliveryResult.failure(FailureKinds.of(e), FailureKinds.describe(e), null, null);
        }
    }

    /** 연결 테스트: 새 클라이언트로 접속해 메시지 하나를 보내고 끊는다(API-DSC-76) */
    public DeliveryResult test(OutputConnection c, OutboundMessage m) {
        if (HostGuard.denied(c.url(), deniedHosts)) {
            return DeliveryResult.failure(OutputFailureKind.REFUSED, "FORBIDDEN_HOST", null, null);
        }
        Mqtt5AsyncClient client = null;
        try {
            client = connect(c, clientIdBase + "-test-" + java.util.UUID.randomUUID().toString().substring(0, 8));
            send(client, c, m);
            return DeliveryResult.success(null, null);
        } catch (Exception e) {
            return DeliveryResult.failure(FailureKinds.of(e), FailureKinds.describe(e), null, null);
        } finally {
            if (client != null) {
                disconnect(client);
            }
        }
    }

    private void send(Mqtt5AsyncClient client, OutputConnection c, OutboundMessage m) throws Exception {
        int qos = c.target().path("qos").asInt(1) == 0 ? 0 : 1;
        Mqtt5PublishResult r = client.publishWith().topic(m.topic()).qos(MqttQos.fromCode(qos))
                .retain(c.target().path("retain").asBoolean(false))
                .payload(m.body().getBytes(StandardCharsets.UTF_8)).send()
                .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (r.getError().isPresent()) {
            throw new IllegalStateException("MQTT 발행 실패", r.getError().get());
        }
    }

    private Mqtt5AsyncClient client(OutputConnection c) throws Exception {
        Cached cached = clients.get(c.id());
        if (cached != null && cached.version() == c.version() && cached.client().getState().isConnected()) {
            return cached.client();
        }
        if (cached != null) {
            discard(c.id());
        }
        Mqtt5AsyncClient client = connect(c, clientId(c));
        clients.put(c.id(), new Cached(c.version(), client));
        return client;
    }

    String clientId(OutputConnection c) {
        String configured = c.target().path("clientId").asString("");
        return configured.isBlank() ? clientIdBase + "-out" + c.id() : configured;
    }

    private Mqtt5AsyncClient connect(OutputConnection c, String clientId) throws Exception {
        URI uri = URI.create(c.url().strip());
        String scheme = uri.getScheme() == null ? "mqtt" : uri.getScheme().toLowerCase(Locale.ROOT);
        boolean tls = scheme.equals("mqtts") || scheme.equals("wss") || scheme.equals("ssl");
        boolean ws = scheme.equals("ws") || scheme.equals("wss");
        int port = uri.getPort() > 0 ? uri.getPort() : switch (scheme) {
            case "mqtts", "ssl" -> 8883;
            case "ws" -> 80;
            case "wss" -> 443;
            default -> 1883;
        };
        Mqtt5ClientBuilder b = MqttClient.builder().useMqttVersion5().identifier(clientId).serverHost(uri.getHost()).serverPort(port)
                .transportConfig().socketConnectTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .mqttConnectTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS).applyTransportConfig();
        if (tls) {
            String ca = c.secrets().get("CA_CERT");
            b = ca == null ? b.sslWithDefaultConfig()
                    : b.sslConfig(MqttClientSslConfig.builder().trustManagerFactory(TrustStores.fromPem(ca)).build());
        }
        if (ws) {
            String path = uri.getRawPath() == null || uri.getRawPath().isBlank() ? "/mqtt" : uri.getRawPath();
            b = b.webSocketConfig().serverPath(path.startsWith("/") ? path.substring(1) : path).applyWebSocketConfig();
        }
        Mqtt5AsyncClient client = b.buildAsync();
        var connect = client.connectWith().cleanStart(true);
        String username = c.target().path("username").asString("");
        String password = c.secrets().get("PASSWORD");
        if (!username.isBlank() || password != null) {
            var auth = connect.simpleAuth().username(username);
            if (password != null) {
                auth = auth.password(password.getBytes(StandardCharsets.UTF_8));
            }
            connect = auth.applySimpleAuth();
        }
        connect.send().get(timeout.toMillis() + 1000, TimeUnit.MILLISECONDS);
        return client;
    }

    /** 연결 캐시에서 버린다(정의 변경·삭제·실패) */
    public void discard(long outputId) {
        Cached cached = clients.remove(outputId);
        if (cached != null) {
            disconnect(cached.client());
        }
    }

    private static void disconnect(Mqtt5AsyncClient client) {
        try {
            if (client.getState().isConnected()) {
                client.disconnect().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.debug("출력 MQTT 접속 종료 중 오류: {}", e.toString());
        }
    }

    /** graceful shutdown: 모든 접속을 끊는다 */
    @Override
    public void close() {
        clients.keySet().forEach(this::discard);
    }
}
