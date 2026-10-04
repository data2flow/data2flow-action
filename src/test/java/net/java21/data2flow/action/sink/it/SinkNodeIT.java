package net.java21.data2flow.action.sink.it;

import com.github.dockerjava.api.DockerClient;
import net.java21.data2flow.action.sink.connector.SinkContainers;
import net.java21.data2flow.action.sink.service.SinkConnectionCache;
import net.java21.data2flow.action.sink.service.SinkWriteService;
import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.sink.SinkMode;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.DockerClientFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Sink 노드 통합 시험(FLW-04): 실제 PostgreSQL 18(action)·RabbitMQ 3.13 + 대상 MySQL 8.4·InfluxDB 2 컨테이너. 플로우 엔진 대신 Sink 노드가
 * 내는 행동 요청(kind=SINK)을 {@code data2flow.actions}로 보내고, 연결 정의는 가짜 core(API-FLW-85)가 준다.
 */
@TestPropertySource(properties = {"data2flow.action.sink.connect-timeout=1s", "data2flow.action.sink.connection-ttl=1h"})
class SinkNodeIT extends IntegrationTestSupport {

    private static final long MYSQL_CONNECTION = 4;
    private static final long INFLUX_CONNECTION = 5;
    private final Map<Long, Map<String, Object>> connections = new ConcurrentHashMap<>();

    @Autowired
    SinkWriteService writer;
    @Autowired
    SinkConnectionCache cache;

    @BeforeEach
    void setUp() throws Exception {
        connections.put(MYSQL_CONNECTION, Map.of("connectionId", "4", "organizationId", "1", "name", "외부 MySQL", "type", "MYSQL",
                "config", SinkContainers.MySql.config(), "secrets", Map.of("password", "sink-pass"), "version", 1));
        connections.put(INFLUX_CONNECTION, Map.of("connectionId", "5", "organizationId", "1", "name", "InfluxDB", "type", "INFLUXDB",
                "config", SinkContainers.Influx.config(), "secrets", Map.of("token", SinkContainers.INFLUX_TOKEN), "version", 1));
        CORE.routes.put("/internal/core/sink-connections/", req -> {
            long id = Long.parseLong(req.getPath().substring(req.getPath().lastIndexOf('/') + 1));
            Map<String, Object> c = connections.get(id);
            return c == null ? FakeCore.json(404, Map.of("header", Map.of("isSuccessful", false, "resultCode", "SINK_CONNECTION_NOT_FOUND")))
                    : FakeCore.ok(c);
        });
        cache.invalidateAll();
    }

    private void send(ActionRequest req) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        MessageHeaders.of(req).forEach(props::setHeader);
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, req.routingKey(), new Message(CODEC.write(req), props));
    }

    private static List<Map<String, Object>> records(int n, int offset) {
        List<Map<String, Object>> out = new ArrayList<>();
        Instant t0 = Instant.parse("2026-03-02T00:00:00Z");
        for (int i = 0; i < n; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("device_id", 15 + (i % 3));
            r.put("ts", t0.plusSeconds(offset + i).toString());
            r.put("temperature", 24.0 + (i % 10) * 0.1);
            out.add(r);
        }
        return out;
    }

    /** Sink 노드 하나가 메시지 m에서 낸 요청들(배치마다 하나, 멱등 키에 분할 인덱스 BR-FLW-13) */
    private List<ActionRequest> sinkRequests(long connectionId, String target, String messageId, List<Map<String, Object>> records) {
        List<ActionRequest> out = new ArrayList<>();
        for (SinkWriteRequest b : SinkWriteRequest.batches(connectionId, target, SinkMode.INSERT, List.of(), records, 100)) {
            int index = b.batchIndex() == null ? 0 : b.batchIndex();
            String key = ActionIdempotencyKeys.flow("f-sink", "n-sink-1", messageId, index);
            out.add(ActionRequest.sink(1, key, CommandSource.flow("f-sink", 4, "n-sink-1", messageId), null, b, clock));
        }
        return out;
    }

    private void createTable(String table) {
        Result r = post(api(7, 1), "/internal/action/sinks/connections/4/schema", Map.of("target", table, "columns",
                List.of(Map.of("name", "device_id", "type", "INTEGER"), Map.of("name", "ts", "type", "TIMESTAMP"),
                        Map.of("name", "temperature", "type", "NUMBER"))), null);
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.response().path("exists").asBoolean()).isTrue();
    }

    private static long mysqlCount(String table) throws Exception {
        try (Connection c = DriverManager.getConnection(SinkContainers.MySql.jdbcUrl(), "sink", "sink-pass");
             ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM `" + table + "`")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static long mysqlCountQuietly(String table) {
        try {
            return mysqlCount(table);
        } catch (Exception e) {
            return -1;
        }
    }

    @Test
    @DisplayName("[FLW-04.01][AT-FLW-18.1][TC-FLW-079] MySQL 연결과 Sink 노드에서 메시지 100건 → MySQL에 100행, 같은 요청 재전달에도 100행")
    void mysqlWritesOnce() throws Exception {
        String table = "room_temp_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        Result missing = get(api(7, 1), "/internal/action/sinks/connections/4/schema?target=" + table + "&columns=device_id,ts");
        assertThat(missing.response().path("exists").asBoolean()).isFalse();
        createTable(table);

        ActionRequest req = sinkRequests(MYSQL_CONNECTION, table, "m-1", records(100, 0)).get(0);
        send(req);
        send(req);   // 큐 재전달
        await().atMost(Duration.ofSeconds(30)).until(() -> mysqlCountQuietly(table) == 100);
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'WRITTEN'") == 1);
        assertThat(mysqlCount(table)).isEqualTo(100);
        assertThat(count("SELECT count(*) FROM data2flow_action.executed_actions WHERE kind = 'SINK'")).isEqualTo(1);

        Result test = post(api(7, 1), "/internal/action/sinks/connections/4/test", Map.of(), null);
        assertThat(test.response().path("ok").asBoolean()).isTrue();
        Result draft = post(api(7, 1), "/internal/action/sinks/connections/test", Map.of("type", "MYSQL", "config",
                SinkContainers.MySql.config(), "secrets", Map.of("password", "wrong")), null);
        assertThat(draft.response().path("ok").asBoolean()).isFalse();
        assertThat(draft.response().path("error").path("kind").asString()).isEqualTo("AUTH");
        assertThat(get(api(7, 2), "/internal/action/sinks/connections/4/schema?target=" + table).status()).isEqualTo(404);
    }

    @Test
    @DisplayName("[FLW-04.05][AT-FLW-18.2][TC-FLW-080] 같은 플로우의 연결만 InfluxDB로 바꿔 100건 → InfluxDB에 100포인트")
    void influxWrites() throws Exception {
        String measurement = "room_temp_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        sinkRequests(INFLUX_CONNECTION, measurement, "m-2", records(100, 0)).forEach(this::send);
        await().atMost(Duration.ofSeconds(30)).until(() -> SinkContainers.Influx.count(measurement) == 100);
        assertThat(count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'WRITTEN' AND connection_id = 5")).isEqualTo(1);
    }

    @Test
    @DisplayName("[FLW-04.03][AT-FLW-18.3][TC-FLW-084] 대상 DB 10분 중단 동안 메시지 600건 → 복구 뒤 600건 모두 기록, 유실 0")
    void outageLosesNothing() throws Exception {
        String table = "room_temp_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        createTable(table);
        DockerClient docker = DockerClientFactory.instance().client();
        String id = SinkContainers.MySql.CONTAINER.getContainerId();
        docker.stopContainerCmd(id).exec();
        try {
            sinkRequests(MYSQL_CONNECTION, table, "m-3", records(600, 1000)).forEach(this::send);
            await().atMost(Duration.ofSeconds(60)).until(() ->
                    count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'RETRYING'") == 6);
            for (int minute = 0; minute < 10; minute++) {   // 10분 동안 재시도(MutableClock)
                clock.advanceBy(Duration.ofMinutes(1));
                writer.processDue(50);
            }
            assertThat(count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'DEAD'")).isZero();
            assertThat(count("SELECT min(attempts) FROM data2flow_action.sink_batches")).isGreaterThanOrEqualTo(2);
        } finally {
            docker.startContainerCmd(id).exec();
        }
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1)).until(() -> {
            clock.advanceBy(Duration.ofSeconds(61));
            writer.processDue(50);
            return mysqlCountQuietly(table) == 600;
        });
        assertThat(count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'WRITTEN'")).isEqualTo(6);
    }

    @Test
    @DisplayName("[FLW-04.03][TC-FLW-085] 대상 테이블이 없으면 바로 dead-letter, 목록에 보이고 테이블을 만든 뒤 재전송하면 기록된다")
    void deadLetterAndResend() throws Exception {
        String table = "late_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        sinkRequests(MYSQL_CONNECTION, table, "m-4", records(10, 0)).forEach(this::send);
        await().atMost(Duration.ofSeconds(30)).until(() -> count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'DEAD'") == 1);

        Result list = get(api(7, 1), "/internal/action/sinks/connections/4/dead-letters?size=10");
        assertThat(list.status()).isEqualTo(200);
        assertThat(list.body().path("responses").size()).isEqualTo(1);
        assertThat(list.body().path("responses").get(0).path("errorKind").asString()).isEqualTo("TARGET");
        assertThat(list.body().path("responses").get(0).path("deadUntil").asString()).isNotBlank();

        createTable(table);
        Result resend = post(api(7, 1), "/internal/action/sinks/connections/4/dead-letters/resend", Map.of("all", true), null);
        assertThat(resend.response().path("resent").asInt()).isEqualTo(1);
        writer.processDue(50);
        assertThat(mysqlCount(table)).isEqualTo(10);
        assertThat(post(api(7, 1), "/internal/action/sinks/connections/4/dead-letters/resend", Map.of("all", true), null).status())
                .isEqualTo(404);
    }

    @Test
    @DisplayName("[FLW-04.02] 읽을 수 없는 Sink 요청과 없는 연결: 형식 오류는 DLQ, 연결 없음은 dead-letter")
    void malformedAndUnknownConnection() {
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, "sink", new Message("{\"v\":1}".getBytes(), new MessageProperties()));
        await().atMost(Duration.ofSeconds(20)).until(() -> rabbit.receive("action.sinks.dlq", 100) != null);
        sinkRequests(99, "room_temp", "m-5", records(1, 0)).forEach(this::send);
        await().atMost(Duration.ofSeconds(20)).until(() ->
                count("SELECT count(*) FROM data2flow_action.sink_batches WHERE status = 'DEAD' AND connection_id = 99") == 1);
    }
}
