package net.java21.data2flow.action.sink.connector.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.java21.data2flow.action.sink.connector.ErrorKind;
import net.java21.data2flow.action.sink.connector.Identifiers;
import net.java21.data2flow.action.sink.connector.SinkBatch;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnector;
import net.java21.data2flow.action.sink.connector.SinkTestResult;
import net.java21.data2flow.action.sink.connector.SinkWriteException;
import net.java21.data2flow.action.sink.connector.TargetSchema;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.sink.SinkMode;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * JDBC 저장소 공통(PostgreSQL·MySQL). 연결마다 작은 Hikari 풀을 두고(연결 정의 버전이 바뀌면 다시 만든다), 배치 하나를 트랜잭션 하나로 쓴다.
 *
 * <p>정확히 한 번: 대상 DB에 표시 테이블 {@value #MARKER_TABLE}{@code (idempotency_key PK, written_at)}을 두고, 같은 트랜잭션에서 표시 행을
 * 먼저 넣는다. 표시 행이 이미 있으면 그 배치는 쓴 것이므로 아무것도 하지 않는다(재전달·재시도·파드 재시작에도 중복 없음).
 */
public abstract class JdbcSinkConnector implements SinkConnector {

    public static final String MARKER_TABLE = "data2flow_sink_batches";
    private static final Pattern ISO_INSTANT = Pattern.compile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}(:\\d{2}(\\.\\d{1,9})?)?(Z|[+-]\\d{2}:\\d{2})");

    private final Map<Long, Pool> pools = new ConcurrentHashMap<>();
    private final Clock clock;
    private final int poolSize;
    private final Duration connectTimeout;

    protected JdbcSinkConnector(Clock clock, int poolSize, Duration connectTimeout) {
        this.clock = clock;
        this.poolSize = Math.max(1, poolSize);
        this.connectTimeout = connectTimeout;
    }

    private record Pool(String key, HikariDataSource dataSource) {
    }

    // ───────────── 방언 ─────────────

    /** JDBC URL */
    protected abstract String url(SinkConnection c);

    /** 드라이버 이름 속성 등 추가 속성 */
    protected abstract Properties properties(SinkConnection c);

    /** 기본 스키마(PostgreSQL public, MySQL은 database) */
    protected abstract String defaultSchema(SinkConnection c);

    /** 이름 따옴표 */
    protected abstract String quote(String name);

    /** 표시 행 넣기(이미 있으면 0행) */
    protected abstract String insertMarkerSql();

    /** 표시 테이블 만들기 */
    protected abstract String createMarkerSql();

    /** UPSERT 꼬리(예: ON CONFLICT … DO UPDATE) */
    protected abstract String upsertClause(List<String> columns, List<String> keys);

    /** 표준 형식 → 저장소 형식 */
    protected abstract String sqlType(String type);

    /** 시각 값 묶기 */
    protected abstract Object timestamp(Instant instant);

    /** 저장소 오류를 원인 종류로 */
    protected abstract ErrorKind classifyVendor(SQLException e);

    // ───────────── SPI ─────────────

    @Override
    public SinkTestResult test(SinkConnection c) {
        long start = System.nanoTime();
        try (Connection conn = DriverManager.getConnection(url(c), withTimeouts(c))) {
            try (Statement st = conn.createStatement()) {
                st.execute("SELECT 1");
            }
            return SinkTestResult.ok(elapsed(start));
        } catch (SQLException e) {
            SinkWriteException w = translate(e);
            return SinkTestResult.failed(elapsed(start), w.kind(), w.getMessage());
        } catch (RuntimeException e) {
            return SinkTestResult.failed(elapsed(start), ErrorKind.OTHER, safeMessage(e));
        }
    }

    @Override
    public TargetSchema describe(SinkConnection c, String target) throws SinkWriteException {
        String[] t = Identifiers.table(target);
        String schema = t[0] == null ? defaultSchema(c) : t[0];
        try (Connection conn = pool(c).getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                     SELECT column_name, data_type FROM information_schema.columns
                      WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position""")) {
            ps.setString(1, schema);
            ps.setString(2, t[1]);
            List<TargetSchema.Column> cols = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    cols.add(new TargetSchema.Column(rs.getString(1), rs.getString(2)));
                }
            }
            return cols.isEmpty() ? TargetSchema.missing() : new TargetSchema(true, cols, false);
        } catch (SQLException e) {
            throw translate(e);
        }
    }

    @Override
    public void create(SinkConnection c, String target, List<TargetSchema.Column> columns, List<String> primaryKey)
            throws SinkWriteException {
        if (columns == null || columns.isEmpty()) {
            throw new SinkWriteException(ErrorKind.TARGET, false, "만들 열이 없습니다");
        }
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(qualified(target)).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            TargetSchema.Column col = columns.get(i);
            sql.append(i == 0 ? "" : ", ").append(quote(Identifiers.require(col.name()))).append(' ').append(sqlType(col.type()));
        }
        if (primaryKey != null && !primaryKey.isEmpty()) {
            sql.append(", PRIMARY KEY (");
            for (int i = 0; i < primaryKey.size(); i++) {
                sql.append(i == 0 ? "" : ", ").append(quote(Identifiers.require(primaryKey.get(i))));
            }
            sql.append(')');
        }
        sql.append(')');
        try (Connection conn = pool(c).getConnection(); Statement st = conn.createStatement()) {
            st.execute(sql.toString());
        } catch (SQLException e) {
            throw translate(e);
        }
    }

    @Override
    public WriteOutcome write(SinkConnection c, SinkBatch batch) throws SinkWriteException {
        String table = qualified(batch.target());
        List<String> columns = batch.columns();
        for (String col : columns) {
            Identifiers.require(col);
        }
        for (String key : batch.upsertKeys()) {
            Identifiers.require(key);
        }
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            sql.append(i == 0 ? "" : ", ").append(quote(columns.get(i)));
        }
        sql.append(") VALUES (").append("?, ".repeat(columns.size() - 1)).append("?)");
        if (batch.mode() == SinkMode.UPSERT) {
            sql.append(' ').append(upsertClause(columns, batch.upsertKeys()));
        }
        try (Connection conn = pool(c).getConnection()) {
            conn.setAutoCommit(false);
            try {
                ensureMarkerTable(conn);
                try (PreparedStatement marker = conn.prepareStatement(insertMarkerSql())) {
                    marker.setString(1, batch.idempotencyKey());
                    marker.setObject(2, timestamp(clock.instant()));
                    if (marker.executeUpdate() == 0) {
                        conn.rollback();
                        return WriteOutcome.ALREADY_WRITTEN;
                    }
                }
                try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                    for (Map<String, Object> r : batch.records()) {
                        for (int i = 0; i < columns.size(); i++) {
                            ps.setObject(i + 1, value(r.get(columns.get(i))));
                        }
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
                conn.commit();
                return WriteOutcome.WRITTEN;
            } catch (SQLException e) {
                rollbackQuietly(conn);
                throw e;
            }
        } catch (SQLException e) {
            throw translate(e);
        }
    }

    @Override
    public void release(long connectionId) {
        Pool p = pools.remove(connectionId);
        if (p != null) {
            p.dataSource().close();
        }
    }

    @Override
    public void close() {
        pools.keySet().forEach(this::release);
    }

    // ───────────── 도움 ─────────────

    private void ensureMarkerTable(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(createMarkerSql());
        }
    }

    private String qualified(String target) throws SinkWriteException {
        String[] t = Identifiers.table(target);
        return t[0] == null ? quote(t[1]) : quote(t[0]) + "." + quote(t[1]);
    }

    private HikariDataSource pool(SinkConnection c) {
        String key = c.poolKey();
        Pool p = pools.compute(c.connectionId(), (id, old) -> {
            if (old != null && old.key().equals(key) && !old.dataSource().isClosed()) {
                return old;
            }
            if (old != null) {
                old.dataSource().close();
            }
            return new Pool(key, newPool(c));
        });
        return p.dataSource();
    }

    private HikariDataSource newPool(SinkConnection c) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName("sink-" + c.connectionId());
        cfg.setJdbcUrl(url(c));
        cfg.setDataSourceProperties(withTimeouts(c));
        cfg.setMaximumPoolSize(poolSize);
        cfg.setMinimumIdle(0);
        cfg.setConnectionTimeout(Math.max(250, connectTimeout.toMillis()));
        cfg.setInitializationFailTimeout(-1);   // 대상이 멈춰 있어도 풀은 만든다(쓰기 때 일시 실패로 재시도)
        cfg.setIdleTimeout(60_000);
        return new HikariDataSource(cfg);
    }

    private Properties withTimeouts(SinkConnection c) {
        Properties p = properties(c);
        String user = c.string("username", null);
        if (user != null) {
            p.setProperty("user", user);
        }
        String password = c.secret("password");
        if (password != null) {
            p.setProperty("password", password);
        }
        return p;
    }

    protected Duration connectTimeout() {
        return connectTimeout;
    }

    /** JSON 원시값 → JDBC 값. ISO-8601 시각 문자열은 시각으로, 객체·배열은 JSON 문자열로 */
    protected Object value(Object v) {
        if (v instanceof String s && ISO_INSTANT.matcher(s).matches()) {
            try {
                return timestamp(OffsetDateTime.parse(s).toInstant());
            } catch (DateTimeParseException e) {
                return s;
            }
        }
        if (v instanceof Map<?, ?> || v instanceof List<?>) {
            return Json.write(v);
        }
        return v;
    }

    protected SinkWriteException translate(SQLException e) {
        ErrorKind kind = classify(e);
        boolean transientFailure = kind == ErrorKind.TIMEOUT || kind == ErrorKind.REFUSED || kind == ErrorKind.OTHER
                && (e instanceof SQLTransientException || state(e).startsWith("08") || state(e).startsWith("40"));
        if (kind == ErrorKind.DNS) {
            transientFailure = true;   // DNS 일시 장애일 수 있다. 오래 가면 재시도 기한(maxRetryAge) 뒤 dead-letter
        }
        return new SinkWriteException(kind, transientFailure, safeMessage(e), e);
    }

    private ErrorKind classify(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof UnknownHostException) {
                return ErrorKind.DNS;
            }
            if (t instanceof javax.net.ssl.SSLException) {
                return ErrorKind.TLS;
            }
            if (t instanceof SocketTimeoutException || t instanceof SQLTimeoutException) {
                return ErrorKind.TIMEOUT;
            }
            if (t instanceof ConnectException || t instanceof NoRouteToHostException) {
                return ErrorKind.REFUSED;
            }
        }
        String state = state(e);
        if (state.startsWith("28")) {
            return ErrorKind.AUTH;
        }
        if (state.startsWith("42") || state.startsWith("22") || state.startsWith("23")) {
            return ErrorKind.TARGET;
        }
        if (state.startsWith("08")) {
            return ErrorKind.REFUSED;
        }
        ErrorKind vendor = classifyVendor(e);
        if (vendor != ErrorKind.OTHER) {
            return vendor;
        }
        if (e instanceof java.sql.SQLTransientConnectionException) {
            return ErrorKind.REFUSED;   // 풀이 연결을 얻지 못함(대상 중단)
        }
        return ErrorKind.OTHER;
    }

    protected static String state(SQLException e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException s && s.getSQLState() != null) {
                return s.getSQLState();
            }
        }
        return "";
    }

    private static void rollbackQuietly(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // 연결이 끊긴 경우: 커밋되지 않았으므로 버려진다
        }
    }

    private static long elapsed(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }

    /** 오류 문구(500자, 비밀값은 원래 들어 있지 않음) */
    protected static String safeMessage(Throwable e) {
        String m = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
        return m.length() > 500 ? m.substring(0, 500) : m;
    }
}
