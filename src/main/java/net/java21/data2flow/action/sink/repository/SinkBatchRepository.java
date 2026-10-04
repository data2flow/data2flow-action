package net.java21.data2flow.action.sink.repository;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Sink 쓰기 배치({@code data2flow_action.sink_batches}, BR-FLW-28) */
@Repository
public class SinkBatchRepository {

    private static final String COLUMNS = """
            idempotency_key, organization_id, request_key, connection_id, target, mode, payload, record_count, status, attempts,
            next_retry_at, last_error, error_kind, created_at, written_at, dead_at, dead_until""";

    private final JdbcClient jdbc;

    public SinkBatchRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 받은 배치를 기록한다. 처음이면 true(같은 키는 한 번만, ON CONFLICT DO NOTHING) */
    public boolean insert(long organizationId, String key, String requestKey, SinkWriteRequest request, String source, Instant now,
                          String env) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.sink_batches
                          (idempotency_key, organization_id, request_key, connection_id, target, mode, payload, record_count, status,
                           next_retry_at, created_at, source, env)
                        VALUES (:key, :org, :req, :conn, :target, :mode, CAST(:payload AS jsonb), :count, 'PENDING', :now, :now,
                                CAST(:source AS jsonb), :env)
                        ON CONFLICT (idempotency_key) DO NOTHING""")
                .param("key", key).param("org", organizationId).param("req", requestKey).param("conn", request.connectionId())
                .param("target", request.target()).param("mode", request.mode().name()).param("payload", Json.write(request))
                .param("count", request.records().size()).param("now", Pg.ts(now)).param("source", source).param("env", env)
                .update() == 1;
    }

    public Optional<SinkBatchRow> find(long organizationId, String key) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.sink_batches WHERE organization_id = :org AND idempotency_key = :key")
                .param("org", organizationId).param("key", key).query(SinkBatchRepository::row).optional();
    }

    /**
     * 쓸 차례인 배치(대기·재시도, 시각 도래, 자기 배포)를 잠그고 {@code leaseUntil}까지 다른 파드가 잡지 않게 미룬다. 키 목록을 돌려준다.
     */
    @OrganizationScopeExempt("모든 조직의 재시도 작업(배포 env로만 나눔)")
    public List<SinkBatchRow> lockDue(Instant now, Instant leaseUntil, int limit, String env) {
        List<SinkBatchRow> rows = jdbc.sql("SELECT " + COLUMNS + """
                         FROM data2flow_action.sink_batches
                        WHERE status IN ('PENDING','RETRYING') AND env = :env AND next_retry_at <= :now
                        ORDER BY next_retry_at LIMIT :limit FOR UPDATE SKIP LOCKED""")
                .param("env", env).param("now", Pg.ts(now)).param("limit", limit).query(SinkBatchRepository::row).list();
        for (SinkBatchRow r : rows) {
            jdbc.sql("UPDATE data2flow_action.sink_batches SET next_retry_at = :lease WHERE idempotency_key = :key")
                    .param("lease", Pg.ts(leaseUntil)).param("key", r.key()).update();
        }
        return rows;
    }

    /** 한 행만 잠근다(받은 직후 바로 쓰기). 쓸 상태가 아니면 빈 값 */
    public Optional<SinkBatchRow> lockForWrite(long organizationId, String key, Instant now, Instant leaseUntil) {
        Optional<SinkBatchRow> row = jdbc.sql("SELECT " + COLUMNS + """
                         FROM data2flow_action.sink_batches
                        WHERE organization_id = :org AND idempotency_key = :key AND status IN ('PENDING','RETRYING')
                          AND next_retry_at <= :now
                        FOR UPDATE SKIP LOCKED""")
                .param("org", organizationId).param("key", key).param("now", Pg.ts(now)).query(SinkBatchRepository::row).optional();
        row.ifPresent(r -> jdbc.sql("UPDATE data2flow_action.sink_batches SET next_retry_at = :lease WHERE idempotency_key = :key")
                .param("lease", Pg.ts(leaseUntil)).param("key", key).update());
        return row;
    }

    public void markWritten(long organizationId, String key, int attempts, Instant at) {
        jdbc.sql("""
                        UPDATE data2flow_action.sink_batches
                           SET status = 'WRITTEN', attempts = :attempts, written_at = :at, next_retry_at = NULL, last_error = NULL, error_kind = NULL
                         WHERE organization_id = :org AND idempotency_key = :key""")
                .param("attempts", attempts).param("at", Pg.ts(at)).param("org", organizationId).param("key", key).update();
    }

    public void markRetry(long organizationId, String key, int attempts, Instant nextRetryAt, String errorKind, String error) {
        jdbc.sql("""
                        UPDATE data2flow_action.sink_batches
                           SET status = 'RETRYING', attempts = :attempts, next_retry_at = :next, error_kind = :kind, last_error = :error
                         WHERE organization_id = :org AND idempotency_key = :key""")
                .param("attempts", attempts).param("next", Pg.ts(nextRetryAt)).param("kind", errorKind).param("error", truncate(error))
                .param("org", organizationId).param("key", key).update();
    }

    public void markDead(long organizationId, String key, int attempts, Instant at, Instant deadUntil, String errorKind, String error) {
        jdbc.sql("""
                        UPDATE data2flow_action.sink_batches
                           SET status = 'DEAD', attempts = :attempts, next_retry_at = NULL, dead_at = :at, dead_until = :until,
                               error_kind = :kind, last_error = :error
                         WHERE organization_id = :org AND idempotency_key = :key""")
                .param("attempts", attempts).param("at", Pg.ts(at)).param("until", Pg.ts(deadUntil)).param("kind", errorKind)
                .param("error", truncate(error)).param("org", organizationId).param("key", key).update();
    }

    /** dead-letter 목록(최신순 커서): dead_at·키가 커서보다 앞선 것 */
    public List<SinkBatchRow> findDead(long organizationId, long connectionId, Instant beforeAt, String beforeKey, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM data2flow_action.sink_batches
                        WHERE organization_id = :org AND connection_id = :conn AND status = 'DEAD'
                          AND (CAST(:at AS timestamptz) IS NULL OR (dead_at, idempotency_key) < (CAST(:at AS timestamptz), :key))
                        ORDER BY dead_at DESC, idempotency_key DESC LIMIT :limit""")
                .param("org", organizationId).param("conn", connectionId).param("at", Pg.ts(beforeAt))
                .param("key", beforeKey == null ? "" : beforeKey).param("limit", limit).query(SinkBatchRepository::row).list();
    }

    /** 재전송: DEAD → PENDING(바로 쓸 차례). 바뀐 행 수 */
    public int updateDeadToPending(long organizationId, long connectionId, List<String> keys, Instant now) {
        String filter = keys == null ? "" : " AND idempotency_key IN (:keys)";
        var spec = jdbc.sql("""
                        UPDATE data2flow_action.sink_batches
                           SET status = 'PENDING', next_retry_at = :now, created_at = :now, dead_at = NULL, dead_until = NULL
                         WHERE organization_id = :org AND connection_id = :conn AND status = 'DEAD'""" + filter)
                .param("now", Pg.ts(now)).param("org", organizationId).param("conn", connectionId);
        if (keys != null) {
            if (keys.isEmpty()) {
                return 0;
            }
            spec = spec.param("keys", keys);
        }
        return spec.update();
    }

    /** 보관 정리: 보관이 끝난 dead-letter와 오래된 WRITTEN 행 */
    @OrganizationScopeExempt("모든 조직의 보관 정리")
    public int deleteExpired(Instant now, Instant writtenBefore) {
        return jdbc.sql("""
                        DELETE FROM data2flow_action.sink_batches
                         WHERE (status = 'DEAD' AND dead_until < :now) OR (status = 'WRITTEN' AND written_at < :written)""")
                .param("now", Pg.ts(now)).param("written", Pg.ts(writtenBefore)).update();
    }

    private static SinkBatchRow row(ResultSet rs, int n) throws SQLException {
        return new SinkBatchRow(rs.getString("idempotency_key"), rs.getLong("organization_id"), rs.getString("request_key"),
                rs.getLong("connection_id"), rs.getString("target"), rs.getString("mode"),
                Json.read(rs.getString("payload"), SinkWriteRequest.class), rs.getInt("record_count"), rs.getString("status"),
                rs.getInt("attempts"), Pg.instant(rs, "next_retry_at"), rs.getString("last_error"), rs.getString("error_kind"),
                Pg.instant(rs, "created_at"), Pg.instant(rs, "written_at"), Pg.instant(rs, "dead_at"), Pg.instant(rs, "dead_until"));
    }

    private static String truncate(String s) {
        return s == null || s.length() <= 500 ? s : s.substring(0, 500);
    }

    /** 배치 행 */
    public record SinkBatchRow(String key, long organizationId, String requestKey, long connectionId, String target, String mode,
                               SinkWriteRequest request, int recordCount, String status, int attempts, Instant nextRetryAt,
                               String lastError, String errorKind, Instant createdAt, Instant writtenAt, Instant deadAt,
                               Instant deadUntil) {
    }
}
