package net.java21.data2flow.action.notification.repository;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** 알림 발송 기록(notification_deliveries, ERD action.md §4.1) */
@Repository
public class DeliveryRepository {

    private static final String COLUMNS = """
            id, organization_id, idempotency_key, channel_id, channel_type, source_type, source_id, alarm_id, aggregate_id, recipient_key,
            user_id, request_key, event, severity, step_no, payload::text AS payload, digest_count, status, skip_reason, attempt,
            next_retry_at, last_error, external_message_id, created_at, sent_at""";

    private final JdbcClient jdbc;

    public DeliveryRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 처음이면 기록하고 true(같은 멱등 키는 한 번만, BR-RUL-17) */
    public boolean insert(Delivery d, String env) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.notification_deliveries (id, organization_id, idempotency_key, channel_id, channel_type,
                            source_type, source_id, alarm_id, aggregate_id, recipients, subject, body_preview, digest_count, status, skip_reason,
                            attempt, next_retry_at, last_error, created_at, request_key, event, severity, recipient_key, user_id, payload, step_no, env)
                        VALUES (:id, :org, :key, :channelId, :channelType, :sourceType, :sourceId, :alarmId, :aggregateId, CAST(:recipients AS jsonb),
                            :subject, :preview, :digest, :status, :skip, 0, :next, :lastError, :createdAt, :requestKey, :event, :severity,
                            :recipientKey, :userId, CAST(:payload AS jsonb), :stepNo, :env)
                        ON CONFLICT (idempotency_key) DO NOTHING""")
                .param("id", d.id()).param("org", d.organizationId()).param("key", d.idempotencyKey()).param("channelId", d.channelId())
                .param("channelType", d.channelType()).param("sourceType", d.sourceType()).param("sourceId", d.sourceId())
                .param("alarmId", d.alarmId()).param("aggregateId", d.aggregateId())
                .param("recipients", Json.write(List.of(Map.of("key", d.recipientKey(),
                        "address", d.payload() == null || d.payload().address() == null ? "" : d.payload().address()))))
                .param("subject", cut(d.payload() == null ? null : d.payload().title(), 200))
                .param("preview", cut(d.payload() == null ? null : d.payload().body(), 200))
                .param("digest", d.digestCount()).param("status", d.status().name()).param("skip", d.skipReason())
                .param("next", Pg.ts(d.nextRetryAt())).param("lastError", d.lastError()).param("createdAt", Pg.ts(d.createdAt()))
                .param("requestKey", d.requestKey()).param("event", d.event()).param("severity", d.severity())
                .param("recipientKey", cut(d.recipientKey(), 160)).param("userId", d.userId())
                .param("payload", d.payload() == null ? null : Json.write(d.payload())).param("stepNo", d.stepNo()).param("env", env)
                .update() == 1;
    }

    public Optional<Delivery> findByIdAndOrganizationId(UUID id, long organizationId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.notification_deliveries WHERE id = :id AND organization_id = :org")
                .param("id", id).param("org", organizationId).query(DeliveryRepository::map).optional();
    }

    public Optional<Delivery> findByKey(long organizationId, String idempotencyKey) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.notification_deliveries WHERE organization_id = :org AND idempotency_key = :key")
                .param("org", organizationId).param("key", idempotencyKey).query(DeliveryRepository::map).optional();
    }

    /** 콜백에서 발송 ID로 찾기(조직은 콜백 뒤에 연결 계정으로 확인한다) */
    @OrganizationScopeExempt("메신저 콜백은 조직을 모른 채 들어온다. 찾은 행의 조직을 연결 계정의 조직과 비교한다")
    public Optional<Delivery> findForCallback(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.notification_deliveries WHERE id = :id")
                .param("id", id).query(DeliveryRepository::map).optional();
    }

    /** 같은 알람·수신자·채널의 마지막 SENT 시각(BR-RUL-13) */
    public Optional<Instant> lastSentAt(long organizationId, long alarmId, String recipientKey, String channelType) {
        return jdbc.sql("""
                        SELECT max(sent_at) FROM data2flow_action.notification_deliveries
                         WHERE organization_id = :org AND alarm_id = :alarm AND recipient_key = :rk AND channel_type = :ct AND status = 'SENT'""")
                .param("org", organizationId).param("alarm", alarmId).param("rk", recipientKey).param("ct", channelType)
                .query((rs, n) -> Optional.ofNullable(Pg.instant(rs, "max"))).single();
    }

    /** 채널 분당 한도 판정: 이 시각 이후 보냈거나 지금 보낼 예정인 건수(OPS-06.04) */
    public int countSentOrDueSince(long organizationId, long channelId, Instant since) {
        Integer n = jdbc.sql("""
                        SELECT count(*) FROM data2flow_action.notification_deliveries
                         WHERE organization_id = :org AND channel_id = :ch
                           AND ((status = 'SENT' AND sent_at >= :since) OR (status = 'PENDING' AND next_retry_at IS NOT NULL AND created_at >= :since))""")
                .param("org", organizationId).param("ch", channelId).param("since", Pg.ts(since)).query(Integer.class).single();
        return n == null ? 0 : n;
    }

    /** 보낼 때가 된 행(재시도 작업). 다른 파드와 겹치지 않게 잠근다 */
    @OrganizationScopeExempt("재시도 작업은 이 배포의 모든 조직 발송을 처리한다")
    public List<UUID> lockDue(Instant now, int limit, String env) {
        return jdbc.sql("""
                        SELECT id FROM data2flow_action.notification_deliveries
                         WHERE status IN ('PENDING','RETRYING') AND next_retry_at <= :now AND env = :env
                         ORDER BY next_retry_at LIMIT :limit FOR UPDATE SKIP LOCKED""")
                .param("now", Pg.ts(now)).param("env", env).param("limit", limit).query(UUID.class).list();
    }

    /** 보낼 행 하나를 잠근다(보낼 상태이고 때가 됐을 때만) */
    @OrganizationScopeExempt("발송 ID(UUID)로 잠근다. 행은 만든 조직의 것만 들어 있다")
    public Optional<Delivery> lockSendable(UUID id, Instant now) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM data2flow_action.notification_deliveries
                         WHERE id = :id AND status IN ('PENDING','RETRYING') AND next_retry_at IS NOT NULL AND next_retry_at <= :now
                         FOR UPDATE SKIP LOCKED""")
                .param("id", id).param("now", Pg.ts(now)).query(DeliveryRepository::map).optional();
    }

    /** 보내는 중 표시: 호출 횟수를 올리고 다음 시각을 미뤄 다른 파드가 겹쳐 부르지 않게 한다 */
    @OrganizationScopeExempt("잠근 행을 ID로 갱신")
    public void markAttempt(UUID id, int attempt, Instant leaseUntil) {
        jdbc.sql("UPDATE data2flow_action.notification_deliveries SET attempt = :a, next_retry_at = :n WHERE id = :id")
                .param("a", attempt).param("n", Pg.ts(leaseUntil)).param("id", id).update();
    }

    @OrganizationScopeExempt("잠근 행을 ID로 갱신")
    public void markSent(UUID id, Instant at, String externalMessageId) {
        jdbc.sql("""
                        UPDATE data2flow_action.notification_deliveries
                           SET status = 'SENT', sent_at = :at, next_retry_at = NULL, last_error = NULL, external_message_id = :ext WHERE id = :id""")
                .param("at", Pg.ts(at)).param("ext", externalMessageId).param("id", id).update();
    }

    @OrganizationScopeExempt("잠근 행을 ID로 갱신")
    public void markRetry(UUID id, Instant next, String error) {
        jdbc.sql("UPDATE data2flow_action.notification_deliveries SET status = 'RETRYING', next_retry_at = :n, last_error = :e WHERE id = :id")
                .param("n", Pg.ts(next)).param("e", cut(error, 500)).param("id", id).update();
    }

    @OrganizationScopeExempt("잠근 행을 ID로 갱신")
    public void markFailed(UUID id, String error) {
        jdbc.sql("UPDATE data2flow_action.notification_deliveries SET status = 'FAILED', next_retry_at = NULL, last_error = :e WHERE id = :id")
                .param("e", cut(error, 500)).param("id", id).update();
    }

    /** 메시지 갱신 결과(버튼 응답 뒤 같은 메시지 수정, BR-RUL-18) */
    @OrganizationScopeExempt("잠근 행을 ID로 갱신")
    public void updatePayload(UUID id, MessagePayload payload) {
        jdbc.sql("UPDATE data2flow_action.notification_deliveries SET payload = CAST(:p AS jsonb) WHERE id = :id")
                .param("p", Json.write(payload)).param("id", id).update();
    }

    /** 묶음에서 기다리는 행(보낼 시각이 없음) */
    public List<Delivery> lockHeld(long organizationId, long aggregateId) {
        return jdbc.sql("SELECT " + COLUMNS + """
                         FROM data2flow_action.notification_deliveries
                         WHERE organization_id = :org AND aggregate_id = :agg AND status = 'PENDING' AND next_retry_at IS NULL
                         ORDER BY created_at, id FOR UPDATE""")
                .param("org", organizationId).param("agg", aggregateId).query(DeliveryRepository::map).list();
    }

    public void release(long organizationId, UUID id, Instant at) {
        jdbc.sql("UPDATE data2flow_action.notification_deliveries SET next_retry_at = :at WHERE id = :id AND organization_id = :org")
                .param("at", Pg.ts(at)).param("id", id).param("org", organizationId).update();
    }

    public void markDigested(long organizationId, long aggregateId) {
        jdbc.sql("""
                        UPDATE data2flow_action.notification_deliveries SET status = 'DIGESTED'
                         WHERE organization_id = :org AND aggregate_id = :agg AND status = 'PENDING' AND next_retry_at IS NULL""")
                .param("org", organizationId).param("agg", aggregateId).update();
    }

    /** 발송 이력 커서 목록(API-RUL-27, 최신순) */
    public List<Delivery> page(DeliveryQuery q, Instant cursorAt, UUID cursorId, int limit) {
        StringBuilder sql = new StringBuilder("SELECT " + COLUMNS + " FROM data2flow_action.notification_deliveries WHERE organization_id = :org");
        if (q.alarmId() != null) {
            sql.append(" AND alarm_id = :alarm");
        }
        if (q.channelId() != null) {
            sql.append(" AND channel_id = :ch");
        }
        if (q.status() != null) {
            sql.append(" AND status = :status");
        }
        if (q.from() != null) {
            sql.append(" AND created_at >= :from");
        }
        if (q.to() != null) {
            sql.append(" AND created_at < :to");
        }
        if (cursorAt != null) {
            sql.append(" AND (created_at, id) < (:cAt, :cId)");
        }
        sql.append(" ORDER BY created_at DESC, id DESC LIMIT :limit");
        var spec = jdbc.sql(sql.toString()).param("org", q.organizationId()).param("limit", limit);
        if (q.alarmId() != null) {
            spec = spec.param("alarm", q.alarmId());
        }
        if (q.channelId() != null) {
            spec = spec.param("ch", q.channelId());
        }
        if (q.status() != null) {
            spec = spec.param("status", q.status());
        }
        if (q.from() != null) {
            spec = spec.param("from", Pg.ts(q.from()));
        }
        if (q.to() != null) {
            spec = spec.param("to", Pg.ts(q.to()));
        }
        if (cursorAt != null) {
            spec = spec.param("cAt", Pg.ts(cursorAt)).param("cId", cursorId);
        }
        return spec.query(DeliveryRepository::map).list();
    }

    /** 이력 조회 조건 */
    public record DeliveryQuery(long organizationId, Long alarmId, Long channelId, String status, Instant from, Instant to) {
    }

    static Delivery map(ResultSet rs, int n) throws SQLException {
        String payload = rs.getString("payload");
        int step = rs.getInt("step_no");
        Integer stepNo = rs.wasNull() ? null : step;
        return new Delivery(rs.getObject("id", UUID.class), rs.getLong("organization_id"), rs.getString("idempotency_key").trim(),
                rs.getLong("channel_id"), rs.getString("channel_type"), rs.getString("source_type"), rs.getString("source_id"),
                Pg.longOrNull(rs, "alarm_id"), Pg.longOrNull(rs, "aggregate_id"), rs.getString("recipient_key"), Pg.longOrNull(rs, "user_id"),
                trim(rs.getString("request_key")), rs.getString("event"), rs.getString("severity"), stepNo,
                payload == null ? null : Json.read(payload, MessagePayload.class), rs.getInt("digest_count"),
                DeliveryStatus.valueOf(rs.getString("status")), rs.getString("skip_reason"), rs.getInt("attempt"),
                Pg.instant(rs, "next_retry_at"), rs.getString("last_error"), rs.getString("external_message_id"),
                Pg.instant(rs, "created_at"), Pg.instant(rs, "sent_at"));
    }

    private static String trim(String s) {
        return s == null ? null : s.trim();
    }

    static String cut(String s, int max) {
        return s == null ? null : (s.length() <= max ? s : s.substring(0, max));
    }
}
