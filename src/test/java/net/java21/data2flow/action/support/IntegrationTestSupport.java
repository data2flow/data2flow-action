package net.java21.data2flow.action.support;

import net.java21.data2flow.action.actuation.service.CommandTracker;
import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.action.actuation.service.SandboxRegistry;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.outbox.OutboxRelay;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import net.java21.data2flow.contracts.message.event.EventPayload;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 통합 시험 기반(design/testing/backend.md "서비스 전체 *IT"): 실제 PostgreSQL 18·RabbitMQ 3.13으로 서비스 전체를 띄우고, core·simulator는
 * MockWebServer로 흉내 낸다. 가짜 시뮬레이터의 응답은 실제처럼 {@code data2flow.events}로 발행되어 {@code action.events} 소비 경로를 탄다.
 * 시험마다 action 테이블을 비우고 시계를 T0로 되돌린다.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "management.server.port=0")
@ActiveProfiles("test")
@Import(TestBeans.class)
public abstract class IntegrationTestSupport {

    protected static final FakeCore CORE = new FakeCore();
    protected static final FakeSimulator SIM = new FakeSimulator();
    protected static final MessageCodec CODEC = MessageCodec.create();

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", TestInfrastructure.POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", TestInfrastructure.POSTGRES::getUsername);
        registry.add("spring.datasource.password", TestInfrastructure.POSTGRES::getPassword);
        registry.add("spring.rabbitmq.host", TestInfrastructure.RABBIT::getHost);
        registry.add("spring.rabbitmq.port", TestInfrastructure::amqpPort);
        registry.add("spring.rabbitmq.virtual-host", () -> "/");
        registry.add("spring.rabbitmq.username", () -> "guest");
        registry.add("spring.rabbitmq.password", () -> "guest");
        registry.add("data2flow.action.core-uri", CORE::url);
        registry.add("data2flow.action.simulator-uri", SIM::url);
    }

    @Autowired
    protected MutableClock clock;
    @Autowired
    protected Clock appClock;
    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected RabbitTemplate rabbit;
    @Autowired
    protected RabbitAdmin rabbitAdmin;
    @Autowired
    protected OutboxRelay relay;
    @Autowired
    protected CommandTracker tracker;
    @Autowired
    protected ControlProfileService profiles;
    @Autowired
    protected SandboxRegistry sandbox;
    @LocalServerPort
    protected int port;

    protected String eventsQueue;
    private final List<JsonNode> received = new ArrayList<>();

    @BeforeEach
    void resetState() {
        clock.set(MutableClock.T0);
        // 스키마의 모든 업무 테이블을 비운다(M4 sink·notification 패키지가 더한 테이블 포함, Flyway 이력 제외)
        List<String> tables = jdbc.sql("""
                SELECT tablename FROM pg_tables WHERE schemaname = 'data2flow_action' AND tablename <> 'flyway_schema_history'
                  AND tablename NOT LIKE 'device_state_history_%'
                """).query(String.class).list();
        jdbc.sql("TRUNCATE " + String.join(", ", tables.stream().map(t -> "data2flow_action." + t).toList()) + " CASCADE").update();
        CORE.reset();
        SIM.reset();
        profiles.invalidateAll();
        sandbox.invalidate();
        SIM.events = new FakeSimulator.Events() {
            @Override
            public void ack(long deviceId, String commandId, boolean acked) {
                publish(EventType.DEVICE_COMMAND_ACK, acked ? DeviceCommandAck.acked(commandId, deviceId, clock.instant(), true)
                        : DeviceCommandAck.failed(commandId, deviceId, "INVALID_COMMAND", clock.instant(), true));
            }

            @Override
            public void reported(long deviceId, long version, Map<String, Map<String, Object>> capabilities) {
                publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(deviceId, version, capabilities, clock.instant(), true));
            }
        };
        Queue queue = new Queue("test.action.events." + UUID.randomUUID(), false, false, false);
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false))
                .with("command.status.#"));
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false))
                .with("device.state.changed"));
        eventsQueue = queue.getName();
        received.clear();
    }

    @AfterEach
    void cleanup() {
        if (eventsQueue != null) {
            rabbitAdmin.deleteQueue(eventsQueue);
        }
    }

    /** 장비·시뮬레이터가 내는 이벤트(EVT-SIM-03)를 data2flow.events로 발행 */
    protected void publish(EventType type, EventPayload payload) {
        publish(type, 1L, payload);
    }

    protected void publish(EventType type, long organizationId, EventPayload payload) {
        DomainEvent<EventPayload> event = DomainEvent.of(type, organizationId, payload, null, clock);
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        MessageHeaders.of(event).forEach(props::setHeader);
        rabbit.send(MessagingNames.EXCHANGE_EVENTS, type.routingKey(), new Message(CODEC.write(event), props));
    }

    /** core가 부르는 모양(X-CALLER-SERVICE + 신원 헤더) */
    protected RestClient api(long userId, long organizationId) {
        return RestClient.builder().baseUrl("http://localhost:" + port)
                .defaultHeader(DataflowHeaders.CALLER_SERVICE, "data2flow-core-api")
                .defaultHeader(DataflowHeaders.USER_ID, Long.toString(userId))
                .defaultHeader(DataflowHeaders.ORG_ID, Long.toString(organizationId))
                .defaultStatusHandler(HttpStatusCode::isError, (req, res) -> {
                })
                .build();
    }

    protected Result post(RestClient client, String path, Object body, String idempotencyKey) {
        return exchange(client.post().uri(path).contentType(MediaType.APPLICATION_JSON)
                .headers(h -> {
                    if (idempotencyKey != null) {
                        h.set(DataflowHeaders.IDEMPOTENCY_KEY, idempotencyKey);
                    }
                }).body(body));
    }

    protected Result get(RestClient client, String path) {
        return exchange(client.get().uri(path));
    }

    protected Result exchange(RestClient.RequestHeadersSpec<?> spec) {
        return spec.exchange((req, res) -> {
            byte[] bytes = res.getBody().readAllBytes();
            JsonNode body = bytes.length == 0 ? null : Json.MAPPER.readTree(bytes);
            return new Result(res.getStatusCode().value(), body, res.getHeaders().getFirst("Retry-After"));
        });
    }

    /** 응답 */
    public record Result(int status, JsonNode body, String retryAfter) {
        public JsonNode response() {
            return body.get("response");
        }

        public String code() {
            return body.path("header").path("resultCode").asString();
        }
    }

    /** 아웃박스를 비우고(릴레이) 지금까지 받은 이벤트를 라우팅 키로 거른다 */
    protected List<JsonNode> events(String type) {
        while (relay.relayOnce() > 0) {
            // 남은 행이 없을 때까지
        }
        org.springframework.amqp.core.Message m;
        while ((m = rabbit.receive(eventsQueue, 200)) != null) {
            received.add(Json.MAPPER.readTree(m.getBody()));
        }
        return received.stream().filter(e -> type == null || e.path("type").asString().equals(type)).toList();
    }

    protected String status(String commandId) {
        return jdbc.sql("SELECT status FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)").param("id", commandId)
                .query(String.class).single();
    }

    protected long count(String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }
}
