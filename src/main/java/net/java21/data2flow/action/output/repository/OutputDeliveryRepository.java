package net.java21.data2flow.action.output.repository;

import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** 출력 연결 발송 대기열({@code data2flow_action.output_deliveries})과 발송 리스({@code output_sender_leases}), BR-DSC-19 */
@Repository
public class OutputDeliveryRepository {

    private static final String COLUMNS = """
            id, organization_id, output_id, message_id, part, device_id, measured_at, topic, body, status, attempts, next_attempt_at,
            retry_started_at, created_at""";

    private final JdbcClient jdbc;

    public OutputDeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 쌓을 행 */
    public record NewDelivery(long organizationId, long outputId, UUID messageId, String part, long deviceId, Instant measuredAt,
                              String topic, String body) {
    }

    /** 대기열 행 */
    public record DeliveryRow(long id, long organizationId, long outputId, UUID messageId, String part, long deviceId, Instant measuredAt,
                              String topic, String body, String status, int attempts, Instant nextAttemptAt, Instant retryStartedAt,
                              Instant createdAt) {
    }

    /** 쌓는다. 같은 (연결, 메시지, 부분)은 한 번만(재전달·재시작에도 멱등). 새로 쌓은 수 */
    public int insert(long organizationId, NewDelivery d, String env, Instant now) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.output_deliveries
                          (organization_id, output_id, env, message_id, part, device_id, measured_at, topic, body, status, attempts,
                           next_attempt_at, retry_started_at, created_at)
                        VALUES (:org, :output, :env, :msg, :part, :device, :measured, :topic, :body, 'PENDING', 0, :now, :now, :now)
                        ON CONFLICT (output_id, message_id, part) DO NOTHING""")
                .param("org", organizationId).param("output", d.outputId()).param("env", env).param("msg", d.messageId())
                .param("part", d.part()).param("device", d.deviceId()).param("measured", Pg.ts(d.measuredAt())).param("topic", d.topic())
                .param("body", d.body()).param("now", Pg.ts(now)).update();
    }

    /** 대기 행이 있는 연결(자기 배포) */
    @OrganizationScopeExempt("발송 작업은 이 배포의 모든 조직 연결을 돈다(배포 env로만 나눔)")
    public List<long[]> listPendingOutputs(String env) {
        return jdbc.sql("SELECT DISTINCT output_id, organization_id FROM data2flow_action.output_deliveries WHERE env = :env AND status = 'PENDING'")
                .param("env", env).query((rs, n) -> new long[]{rs.getLong(1), rs.getLong(2)}).list();
    }

    /**
     * 연결 발송 리스를 잡는다(없거나 끝났거나 내 것이면). 잡았으면 true. 연결마다 한 파드만 보내 순서를 지킨다.
     */
    public boolean acquireLease(long organizationId, long outputId, String env, String holder, Instant now, Instant leaseUntil) {
        return !jdbc.sql("""
                        INSERT INTO data2flow_action.output_sender_leases (output_id, env, organization_id, holder, lease_until)
                        VALUES (:output, :env, :org, :holder, :until)
                        ON CONFLICT (output_id, env) DO UPDATE SET holder = EXCLUDED.holder, lease_until = EXCLUDED.lease_until
                         WHERE data2flow_action.output_sender_leases.lease_until < :now
                            OR data2flow_action.output_sender_leases.holder = EXCLUDED.holder
                        RETURNING holder""")
                .param("output", outputId).param("env", env).param("org", organizationId).param("holder", holder)
                .param("until", Pg.ts(leaseUntil)).param("now", Pg.ts(now)).query(String.class).list().isEmpty();
    }

    /** 리스를 놓는다(종료) */
    @OrganizationScopeExempt("종료할 때 이 파드가 잡은 리스를 모두 놓는다")
    public void releaseLeases(String env, String holder) {
        jdbc.sql("DELETE FROM data2flow_action.output_sender_leases WHERE env = :env AND holder = :holder")
                .param("env", env).param("holder", holder).update();
    }

    /** 연결 맨 앞의 대기 행들(id 순서) */
    public List<DeliveryRow> listHead(long organizationId, long outputId, String env, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM data2flow_action.output_deliveries
                        WHERE organization_id = :org AND output_id = :output AND env = :env AND status = 'PENDING'
                        ORDER BY id LIMIT :limit""")
                .param("org", organizationId).param("output", outputId).param("env", env).param("limit", limit)
                .query(OutputDeliveryRepository::row).list();
    }

    public void updateSent(long organizationId, List<Long> ids, Instant now) {
        if (ids.isEmpty()) {
            return;
        }
        jdbc.sql("""
                        UPDATE data2flow_action.output_deliveries SET status = 'SENT', sent_at = :now, attempts = attempts + 1,
                               last_error = NULL, failure_kind = NULL
                         WHERE organization_id = :org AND id = ANY(:ids)""")
                .param("org", organizationId).param("ids", ids.toArray(Long[]::new)).param("now", Pg.ts(now)).update();
    }

    /** 실패: 시도 수 +1, 다음 시도 시각 */
    public void updateRetry(long organizationId, long id, Instant nextAttemptAt, String error, String kind) {
        jdbc.sql("""
                        UPDATE data2flow_action.output_deliveries SET attempts = attempts + 1, next_attempt_at = :next,
                               last_error = :error, failure_kind = :kind
                         WHERE organization_id = :org AND id = :id""")
                .param("org", organizationId).param("id", id).param("next", Pg.ts(nextAttemptAt)).param("error", cut(error))
                .param("kind", kind).update();
    }

    /** 기한(24시간)이 지나 실패 보관함으로 */
    public void updateFailed(long organizationId, long id, Instant now, String error, String kind) {
        jdbc.sql("""
                        UPDATE data2flow_action.output_deliveries SET status = 'FAILED', attempts = attempts + 1, failed_at = :now,
                               last_error = :error, failure_kind = :kind
                         WHERE organization_id = :org AND id = :id""")
                .param("org", organizationId).param("id", id).param("now", Pg.ts(now)).param("error", cut(error)).param("kind", kind)
                .update();
    }

    /** 연결이 지워져 보낼 수 없는 대기 행을 실패 보관함으로 */
    public int updateOrphaned(long organizationId, long outputId, String env, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_action.output_deliveries SET status = 'FAILED', failed_at = :now, last_error = 'CONNECTION_REMOVED'
                         WHERE organization_id = :org AND output_id = :output AND env = :env AND status = 'PENDING'""")
                .param("org", organizationId).param("output", outputId).param("env", env).param("now", Pg.ts(now)).update();
    }

    /** 실패 보관함 재전송(API-DSC-77): 쌓은 시각이 [from, to)인 FAILED 행을 다시 대기로. 24시간 기한은 지금부터 */
    public int updateReplay(long organizationId, long outputId, Instant from, Instant to, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_action.output_deliveries
                           SET status = 'PENDING', attempts = 0, next_attempt_at = :now, retry_started_at = :now, failed_at = NULL
                         WHERE organization_id = :org AND output_id = :output AND status = 'FAILED'
                           AND created_at >= :from AND created_at < :to""")
                .param("org", organizationId).param("output", outputId).param("from", Pg.ts(from)).param("to", Pg.ts(to))
                .param("now", Pg.ts(now)).update();
    }

    /** 보관 정리: 보냈거나 실패한 지 오래된 행 */
    @OrganizationScopeExempt("모든 조직의 보관 정리(7일)")
    public int deleteExpired(Instant before) {
        return jdbc.sql("""
                        DELETE FROM data2flow_action.output_deliveries
                         WHERE status <> 'PENDING' AND created_at < :before AND COALESCE(sent_at, failed_at, created_at) < :before""")
                .param("before", Pg.ts(before)).update();
    }

    static String cut(String s) {
        return s == null ? null : s.length() > 500 ? s.substring(0, 500) : s;
    }

    static DeliveryRow row(ResultSet rs, int n) throws SQLException {
        return new DeliveryRow(rs.getLong("id"), rs.getLong("organization_id"), rs.getLong("output_id"), rs.getObject("message_id", UUID.class),
                rs.getString("part"), rs.getLong("device_id"), Pg.instant(rs, "measured_at"), rs.getString("topic"), rs.getString("body"),
                rs.getString("status"), rs.getInt("attempts"), Pg.instant(rs, "next_attempt_at"), Pg.instant(rs, "retry_started_at"),
                Pg.instant(rs, "created_at"));
    }
}
