package net.java21.data2flow.action.sink.connector.jdbc;

import net.java21.data2flow.action.sink.connector.ErrorKind;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnectorVerified;
import net.java21.data2flow.contracts.sink.SinkTypes;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

/** PostgreSQL Sink(FLW-04.02). {@code ON CONFLICT}로 UPSERT·표시 행 중복 판정 */
@SinkConnectorVerified("PostgresSinkConnectorContractTest")
public class PostgresSinkConnector extends JdbcSinkConnector {

    public PostgresSinkConnector(Clock clock, int poolSize, Duration connectTimeout) {
        super(clock, poolSize, connectTimeout);
    }

    @Override
    public String type() {
        return SinkTypes.POSTGRESQL;
    }

    @Override
    protected String url(SinkConnection c) {
        return "jdbc:postgresql://" + c.string("host", "localhost") + ":" + c.integer("port", 5432) + "/" + c.string("database", "postgres");
    }

    @Override
    protected Properties properties(SinkConnection c) {
        Properties p = new Properties();
        p.setProperty("ApplicationName", "data2flow-action-sink");
        p.setProperty("connectTimeout", Long.toString(Math.max(1, connectTimeout().toSeconds())));
        p.setProperty("socketTimeout", "30");
        p.setProperty("sslmode", c.flag("ssl") ? "require" : "prefer");
        return p;
    }

    @Override
    protected String defaultSchema(SinkConnection c) {
        return c.string("schema", "public");
    }

    @Override
    protected String quote(String name) {
        return '"' + name + '"';
    }

    @Override
    protected String insertMarkerSql() {
        return "INSERT INTO " + MARKER_TABLE + " (idempotency_key, written_at) VALUES (?, ?) ON CONFLICT (idempotency_key) DO NOTHING";
    }

    @Override
    protected String createMarkerSql() {
        return "CREATE TABLE IF NOT EXISTS " + MARKER_TABLE + " (idempotency_key varchar(64) PRIMARY KEY, written_at timestamptz NOT NULL)";
    }

    @Override
    protected String upsertClause(List<String> columns, List<String> keys) {
        List<String> rest = columns.stream().filter(c -> !keys.contains(c)).toList();
        String conflict = keys.stream().map(this::quote).collect(Collectors.joining(", "));
        if (rest.isEmpty()) {
            return "ON CONFLICT (" + conflict + ") DO NOTHING";
        }
        return "ON CONFLICT (" + conflict + ") DO UPDATE SET "
                + rest.stream().map(c -> quote(c) + " = EXCLUDED." + quote(c)).collect(Collectors.joining(", "));
    }

    @Override
    protected String sqlType(String type) {
        return switch (type == null ? "STRING" : type.toUpperCase(Locale.ROOT)) {
            case "NUMBER", "DOUBLE" -> "double precision";
            case "INTEGER", "LONG" -> "bigint";
            case "BOOLEAN" -> "boolean";
            case "TIMESTAMP" -> "timestamptz";
            case "JSON" -> "jsonb";
            default -> "text";
        };
    }

    @Override
    protected Object timestamp(Instant instant) {
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    @Override
    protected ErrorKind classifyVendor(SQLException e) {
        return ErrorKind.OTHER;
    }
}
