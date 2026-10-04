package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.action.sink.connector.influx.InfluxSinkConnector;
import net.java21.data2flow.action.sink.connector.jdbc.MySqlSinkConnector;
import net.java21.data2flow.action.sink.connector.jdbc.PostgresSinkConnector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SinkConnectorRegistryTest {

    @Test
    @DisplayName("[FLW-04.05][TC-FLW-087] 기본 커넥터 3종(POSTGRESQL·MYSQL·INFLUXDB)이 등록되고, 표시된 계약 시험이 실제로 키트를 상속한다")
    void builtinConnectorsAreVerified() throws Exception {
        Clock clock = Clock.systemUTC();
        List<SinkConnector> builtin = List.of(new PostgresSinkConnector(clock, 1, Duration.ofSeconds(1)),
                new MySqlSinkConnector(clock, 1, Duration.ofSeconds(1)), new InfluxSinkConnector(clock, Duration.ofSeconds(1)));
        SinkConnectorRegistry registry = new SinkConnectorRegistry(builtin);
        assertThat(registry.types()).containsExactly("POSTGRESQL", "MYSQL", "INFLUXDB");
        assertThat(registry.find("mysql")).containsSame(builtin.get(1));
        assertThat(registry.find(null)).isEmpty();
        for (SinkConnector c : builtin) {
            String kit = c.getClass().getAnnotation(SinkConnectorVerified.class).value();
            Class<?> test = Class.forName("net.java21.data2flow.action.sink.connector." + kit);
            assertThat(SinkConnectorContractTest.class.isAssignableFrom(test)).as(kit).isTrue();
        }
        registry.release(9);
        registry.closeAll();
    }

    @Test
    @DisplayName("[FLW-04.05][TC-FLW-087] 키트를 통과하지 않은(표시 없는) 커넥터와 같은 종류 두 개는 등록을 거부한다")
    void unverifiedConnectorRejected() {
        SinkConnector unverified = new SinkConnector() {
            @Override
            public String type() {
                return "KAFKA";
            }

            @Override
            public SinkTestResult test(SinkConnection connection) {
                return SinkTestResult.ok(0);
            }

            @Override
            public TargetSchema describe(SinkConnection connection, String target) {
                return TargetSchema.missing();
            }

            @Override
            public void create(SinkConnection connection, String target, List<TargetSchema.Column> columns, List<String> primaryKey) {
            }

            @Override
            public WriteOutcome write(SinkConnection connection, SinkBatch batch) {
                return WriteOutcome.WRITTEN;
            }
        };
        assertThatThrownBy(() -> new SinkConnectorRegistry(List.of(unverified))).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("계약 키트");
        assertThatThrownBy(() -> new SinkConnectorRegistry(List.of(new InMemorySinkConnector(), new InMemorySinkConnector())))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("[FLW-04.05][TC-FLW-087] 멱등을 어기는 가짜 커넥터는 키트의 '같은 키 재호출 중복 없음' 시나리오에서 실패한다")
    void brokenConnectorFailsKit() {
        BrokenSinkConnector broken = new BrokenSinkConnector();
        SinkConnectorContractTest kit = new SinkConnectorContractTest() {
            @Override
            protected SinkConnector connector() {
                return broken;
            }

            @Override
            protected SinkConnection connection() {
                return new SinkConnection(1, 1, "MEMORY", Map.of(), Map.of(), 1);
            }

            @Override
            protected SinkConnection withWrongCredentials() {
                return connection();
            }

            @Override
            protected SinkConnection withUnknownHost() {
                return connection();
            }

            @Override
            protected SinkConnection withClosedPort() {
                return connection();
            }

            @Override
            protected long count(String target) {
                return broken.count(target);
            }
        };
        assertThatThrownBy(kit::writesBatchOf100Once).isInstanceOf(AssertionError.class);
        assertThatThrownBy(kit::testMapsCauses).isInstanceOf(AssertionError.class);
    }
}
