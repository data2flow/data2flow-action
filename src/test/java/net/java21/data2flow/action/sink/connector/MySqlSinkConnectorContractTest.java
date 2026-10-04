package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.action.sink.connector.jdbc.MySqlSinkConnector;
import net.java21.data2flow.contracts.secret.Secret;
import org.junit.jupiter.api.AfterAll;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/** 계약 키트를 PostgreSQL 커넥터로(Testcontainers MySQL 8.4, 드라이버 MariaDB Connector/J, TC-FLW-087) */
class MySqlSinkConnectorContractTest extends SinkConnectorContractTest {

    private static final MySqlSinkConnector CONNECTOR = new MySqlSinkConnector(Clock.systemUTC(), 2, Duration.ofSeconds(2));

    @AfterAll
    static void close() {
        CONNECTOR.close();
    }

    @Override
    protected SinkConnector connector() {
        return CONNECTOR;
    }

    @Override
    protected SinkConnection connection() {
        return SinkContainers.MySql.connection(1, 1);
    }

    @Override
    protected SinkConnection withWrongCredentials() {
        SinkConnection c = connection();
        return new SinkConnection(91, 1, c.type(), c.config(), Map.of("password", Secret.of("wrong")), 1);
    }

    @Override
    protected SinkConnection withUnknownHost() {
        Map<String, Object> cfg = new HashMap<>(connection().config());
        cfg.put("host", "no-such-host.invalid");
        return new SinkConnection(92, 1, "MYSQL", cfg, connection().secrets(), 1);
    }

    @Override
    protected SinkConnection withClosedPort() {
        Map<String, Object> cfg = new HashMap<>(connection().config());
        cfg.put("host", "127.0.0.1");
        cfg.put("port", 1);
        return new SinkConnection(93, 1, "MYSQL", cfg, connection().secrets(), 1);
    }

    @Override
    protected long count(String target) throws Exception {
        try (Connection c = DriverManager.getConnection(SinkContainers.MySql.jdbcUrl(), "sink", "sink-pass");
             ResultSet rs = c.createStatement().executeQuery("SELECT count(*) FROM `" + target + "`")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
