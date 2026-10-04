package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.action.sink.connector.influx.InfluxSinkConnector;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.sink.SinkMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 계약 키트를 InfluxDB 2 커넥터로(TC-FLW-087). InfluxDB는 스키마가 없어 열 검사는 건너뛰고, 같은 시리즈·시각의 점을 덮어써서 같은 배치를
 * 다시 써도 점 수가 그대로다(자연 멱등).
 */
class InfluxSinkConnectorContractTest extends SinkConnectorContractTest {

    private static final InfluxSinkConnector CONNECTOR = new InfluxSinkConnector(
            Clock.fixed(Instant.parse("2026-03-02T00:00:00Z"), ZoneOffset.UTC), Duration.ofSeconds(2));

    @Override
    protected SinkConnector connector() {
        return CONNECTOR;
    }

    @Override
    protected boolean schemaless() {
        return true;
    }

    @Override
    protected SinkConnection connection() {
        return SinkContainers.Influx.connection(1, 1);
    }

    @Override
    protected SinkConnection withWrongCredentials() {
        return new SinkConnection(91, 1, "INFLUXDB", connection().config(), Map.of("token", Secret.of("wrong")), 1);
    }

    @Override
    protected SinkConnection withUnknownHost() {
        Map<String, Object> cfg = new HashMap<>(connection().config());
        cfg.put("url", "http://no-such-host.invalid:8086");
        return new SinkConnection(92, 1, "INFLUXDB", cfg, connection().secrets(), 1);
    }

    @Override
    protected SinkConnection withClosedPort() {
        Map<String, Object> cfg = new HashMap<>(connection().config());
        cfg.put("url", "http://127.0.0.1:1");
        return new SinkConnection(93, 1, "INFLUXDB", cfg, connection().secrets(), 1);
    }

    @Override
    protected long count(String target) throws Exception {
        return SinkContainers.Influx.count(target);
    }

    @Test
    @DisplayName("[FLW-04.02] 라인 프로토콜: 키는 태그, 시각은 ts, 문자열·정수·실수·참거짓 필드, 이스케이프")
    void lineProtocol() throws Exception {
        String body = CONNECTOR.lines(new SinkBatch(key(), "room_temp", SinkMode.UPSERT, List.of("device_id"),
                List.of(Map.of("device_id", 15, "ts", "2026-03-02T00:00:01Z", "temperature", 24.5, "count", 3, "on", true,
                        "label", "실습실 \"A\""))));
        assertThat(body).startsWith("room_temp,device_id=15 ").contains("temperature=24.5").contains("count=3i").contains("on=true")
                .contains("label=\"실습실 \\\"A\\\"\"").endsWith(" 1772409601000\n");
    }

    @Test
    @DisplayName("[FLW-04.04] 버킷이 없는 연결은 대상이 없다고 알린다")
    void missingBucket() throws Exception {
        Map<String, Object> cfg = new HashMap<>(connection().config());
        cfg.put("bucket", "nope");
        assertThat(CONNECTOR.describe(new SinkConnection(94, 1, "INFLUXDB", cfg, connection().secrets(), 1), "room_temp").exists())
                .isFalse();
    }
}
