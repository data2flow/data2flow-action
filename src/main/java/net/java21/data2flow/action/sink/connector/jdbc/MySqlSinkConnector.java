package net.java21.data2flow.action.sink.connector.jdbc;

import net.java21.data2flow.action.sink.connector.ErrorKind;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnectorVerified;
import net.java21.data2flow.contracts.sink.SinkTypes;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * MySQL Sink(FLW-04.02). 드라이버는 MariaDB Connector/J(LGPL-2.1, 수정 없이 사용 — ADR-018 조건부 허용)이고 MySQL 8 서버에 붙는다.
 * MySQL Connector/J(GPL-2.0 + FOSS 예외)는 허용 목록 밖이라 쓰지 않는다. 시각은 UTC {@code DATETIME(3)}로 쓴다.
 */
@SinkConnectorVerified("MySqlSinkConnectorContractTest")
public class MySqlSinkConnector extends JdbcSinkConnector {

    public MySqlSinkConnector(Clock clock, int poolSize, Duration connectTimeout) {
        super(clock, poolSize, connectTimeout);
    }

    @Override
    public String type() {
        return SinkTypes.MYSQL;
    }

    @Override
    protected String url(SinkConnection c) {
        return "jdbc:mariadb://" + c.string("host", "localhost") + ":" + c.integer("port", 3306) + "/" + c.string("database", "mysql");
    }

    @Override
    protected Properties properties(SinkConnection c) {
        Properties p = new Properties();
        p.setProperty("connectTimeout", Long.toString(Math.max(250, connectTimeout().toMillis())));
        p.setProperty("socketTimeout", "30000");
        // MySQL 8 기본 인증(caching_sha2_password): TLS가 없으면 서버 공개 키로 비밀번호를 암호화한다
        p.setProperty("allowPublicKeyRetrieval", "true");
        p.setProperty("sslMode", c.flag("ssl") ? "verify-full" : "disable");
        p.setProperty("rewriteBatchedStatements", "true");
        return p;
    }

    @Override
    protected String defaultSchema(SinkConnection c) {
        return c.string("database", "mysql");
    }

    @Override
    protected String quote(String name) {
        return '`' + name + '`';
    }

    @Override
    protected String insertMarkerSql() {
        return "INSERT IGNORE INTO " + MARKER_TABLE + " (idempotency_key, written_at) VALUES (?, ?)";
    }

    @Override
    protected String createMarkerSql() {
        return "CREATE TABLE IF NOT EXISTS " + MARKER_TABLE + " (idempotency_key varchar(64) PRIMARY KEY, written_at datetime(3) NOT NULL)";
    }

    @Override
    protected String upsertClause(List<String> columns, List<String> keys) {
        List<String> rest = columns.stream().filter(c -> !keys.contains(c)).toList();
        List<String> set = rest.isEmpty() ? List.of(keys.get(0)) : rest;
        return "ON DUPLICATE KEY UPDATE " + set.stream().map(c -> quote(c) + " = VALUES(" + quote(c) + ")").collect(Collectors.joining(", "));
    }

    @Override
    protected String sqlType(String type) {
        return switch (type == null ? "STRING" : type.toUpperCase(Locale.ROOT)) {
            case "NUMBER", "DOUBLE" -> "double";
            case "INTEGER", "LONG" -> "bigint";
            case "BOOLEAN" -> "boolean";
            case "TIMESTAMP" -> "datetime(3)";
            case "JSON" -> "json";
            default -> "varchar(255)";
        };
    }

    @Override
    protected Object timestamp(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    protected ErrorKind classifyVendor(SQLException e) {
        return switch (e.getErrorCode()) {
            case 1045, 1044, 1698 -> ErrorKind.AUTH;
            case 1146, 1054, 1049 -> ErrorKind.TARGET;
            default -> ErrorKind.OTHER;
        };
    }
}
