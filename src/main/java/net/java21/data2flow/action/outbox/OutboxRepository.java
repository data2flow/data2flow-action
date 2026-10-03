package net.java21.data2flow.action.outbox;

import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

/**
 * 아웃박스({@code data2flow_action.outboxes}, ERD README §11.1). 상태 변경과 같은 트랜잭션에 기록하고, 릴레이가
 * {@code FOR UPDATE SKIP LOCKED}로 가져가 확인(publisher confirm·core 2xx) 뒤 {@code sent_at}을 쓴다. 릴레이는 자기 배포({@code env}) 행만 본다.
 */
@Repository
public class OutboxRepository {

    private final JdbcClient jdbc;

    public OutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 같은 멱등 키가 있으면 무시한다 */
    public void insert(long organizationId, String idempotencyKey, String kind, String exchange, String routingKey, String payloadJson,
                       Instant createdAt, String env) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.outboxes (organization_id, idempotency_key, kind, exchange, routing_key, payload, created_at, env)
                        VALUES (:org, :key, :kind, :exchange, :routingKey, CAST(:payload AS jsonb), :createdAt, :env)
                        ON CONFLICT (idempotency_key) DO NOTHING""")
                .param("org", organizationId).param("key", idempotencyKey).param("kind", kind).param("exchange", exchange)
                .param("routingKey", routingKey).param("payload", payloadJson).param("createdAt", Pg.ts(createdAt)).param("env", env)
                .update();
    }

    @OrganizationScopeExempt("릴레이는 이 배포의 모든 조직 아웃박스를 처리한다")
    public List<OutboxMessage> lockUnsent(int limit, String env) {
        return jdbc.sql("""
                        SELECT id, organization_id, kind, exchange, routing_key, payload::text AS payload, attempts
                          FROM data2flow_action.outboxes
                         WHERE sent_at IS NULL AND env = :env
                         ORDER BY created_at, id
                         LIMIT :limit
                         FOR UPDATE SKIP LOCKED""")
                .param("env", env).param("limit", limit)
                .query((rs, n) -> new OutboxMessage(rs.getLong("id"), rs.getLong("organization_id"), rs.getString("kind"),
                        rs.getString("exchange"), rs.getString("routing_key"), rs.getString("payload"), rs.getInt("attempts")))
                .list();
    }

    @OrganizationScopeExempt("릴레이가 잠근 행을 ID로 갱신")
    public void markSent(long id, Instant sentAt) {
        jdbc.sql("UPDATE data2flow_action.outboxes SET sent_at = :sentAt, attempts = attempts + 1, last_error = NULL WHERE id = :id")
                .param("sentAt", Pg.ts(sentAt)).param("id", id).update();
    }

    @OrganizationScopeExempt("릴레이가 잠근 행을 ID로 갱신")
    public void markFailed(long id, String error) {
        String message = error == null ? "unknown" : (error.length() > 500 ? error.substring(0, 500) : error);
        jdbc.sql("UPDATE data2flow_action.outboxes SET attempts = LEAST(attempts + 1, 32767), last_error = :error WHERE id = :id")
                .param("error", message).param("id", id).update();
    }

    @OrganizationScopeExempt("모든 조직의 보관 정리(보낸 지 7일)")
    public int deleteSentBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM data2flow_action.outboxes WHERE sent_at IS NOT NULL AND sent_at < :cutoff")
                .param("cutoff", Pg.ts(cutoff)).update();
    }

    /** 릴레이가 보낼 한 행 */
    public record OutboxMessage(long id, long organizationId, String kind, String exchange, String routingKey, String payload,
                                int attempts) {
    }
}
