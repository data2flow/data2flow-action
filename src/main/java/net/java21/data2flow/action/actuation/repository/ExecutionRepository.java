package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

/** 행동 멱등 기록({@code executed_actions}, BR-ACT-02)과 큐 소비 중복 판정({@code processed_messages}) */
@Repository
public class ExecutionRepository {

    private final JdbcClient jdbc;

    public ExecutionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ExecutedAction> findExecuted(long organizationId, String idempotencyKey) {
        return jdbc.sql("""
                        SELECT kind, result_ref, result_status FROM data2flow_action.executed_actions
                         WHERE organization_id = :org AND idempotency_key = :key""")
                .param("org", organizationId).param("key", idempotencyKey)
                .query((rs, n) -> new ExecutedAction(rs.getString("kind"), rs.getString("result_ref"), rs.getString("result_status")))
                .optional();
    }

    /** 처음이면 기록하고 true(INSERT … ON CONFLICT DO NOTHING) */
    public boolean recordExecuted(long organizationId, String idempotencyKey, String kind, String resultRef, String resultStatus, Instant at) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.executed_actions (idempotency_key, organization_id, kind, result_ref, result_status, executed_at)
                        VALUES (:key, :org, :kind, :ref, :status, :at) ON CONFLICT (idempotency_key) DO NOTHING""")
                .param("key", idempotencyKey).param("org", organizationId).param("kind", kind).param("ref", resultRef)
                .param("status", resultStatus).param("at", Pg.ts(at))
                .update() == 1;
    }

    /** 큐 메시지를 처음 처리하면 true */
    @OrganizationScopeExempt("소비자·메시지 ID로 판정(조직 무관)")
    public boolean markProcessed(String consumer, String messageId, Instant at) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.processed_messages (consumer, message_id, processed_at) VALUES (:consumer, :id, :at)
                        ON CONFLICT (consumer, message_id) DO NOTHING""")
                .param("consumer", consumer).param("id", messageId).param("at", Pg.ts(at))
                .update() == 1;
    }

    /** 7일 지난 처리 기록 정리 */
    @OrganizationScopeExempt("모든 조직의 보관 정리")
    public int deleteProcessedBefore(Instant cutoff) {
        return jdbc.sql("DELETE FROM data2flow_action.processed_messages WHERE processed_at < :cutoff")
                .param("cutoff", Pg.ts(cutoff)).update();
    }

    public record ExecutedAction(String kind, String resultRef, String resultStatus) {
    }
}
