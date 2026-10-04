package net.java21.data2flow.action.notification.repository;

import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

/** 에스컬레이션 대기(notification_escalations, BR-RUL-16) */
@Repository
public class EscalationRepository {

    private final JdbcClient jdbc;

    public EscalationRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 같은 알람·정책은 한 번만 */
    public boolean insert(long organizationId, long alarmId, long policyId, int nextStep, Instant dueAt, String requestJson, Instant now,
                          String env) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.notification_escalations (organization_id, alarm_id, policy_id, next_step, due_at, request,
                            env, created_at, updated_at)
                        VALUES (:org, :alarm, :policy, :step, :due, CAST(:req AS jsonb), :env, :now, :now)
                        ON CONFLICT (alarm_id, policy_id) DO NOTHING""")
                .param("org", organizationId).param("alarm", alarmId).param("policy", policyId).param("step", nextStep)
                .param("due", Pg.ts(dueAt)).param("req", requestJson).param("env", env).param("now", Pg.ts(now)).update() == 1;
    }

    /** 확인·해제되면 남은 단계 취소 */
    public int cancel(long organizationId, long alarmId, Instant now) {
        return jdbc.sql("""
                        UPDATE data2flow_action.notification_escalations SET status = 'CANCELLED', updated_at = :now
                         WHERE organization_id = :org AND alarm_id = :alarm AND status = 'PENDING'""")
                .param("org", organizationId).param("alarm", alarmId).param("now", Pg.ts(now)).update();
    }

    @OrganizationScopeExempt("에스컬레이션 작업은 이 배포의 모든 조직을 처리한다")
    public List<Escalation> lockDue(Instant now, int limit, String env) {
        return jdbc.sql("""
                        SELECT id, organization_id, alarm_id, policy_id, next_step, due_at, request::text AS request
                          FROM data2flow_action.notification_escalations
                         WHERE status = 'PENDING' AND due_at <= :now AND env = :env
                         ORDER BY due_at LIMIT :limit FOR UPDATE SKIP LOCKED""")
                .param("now", Pg.ts(now)).param("env", env).param("limit", limit).query(EscalationRepository::map).list();
    }

    public void advance(long organizationId, long id, int nextStep, Instant dueAt, Instant now) {
        jdbc.sql("UPDATE data2flow_action.notification_escalations SET next_step = :s, due_at = :d, updated_at = :now WHERE id = :id AND organization_id = :org")
                .param("s", nextStep).param("d", Pg.ts(dueAt)).param("now", Pg.ts(now)).param("id", id).param("org", organizationId).update();
    }

    public void finish(long organizationId, long id, String status, Instant now) {
        jdbc.sql("UPDATE data2flow_action.notification_escalations SET status = :st, updated_at = :now WHERE id = :id AND organization_id = :org")
                .param("st", status).param("now", Pg.ts(now)).param("id", id).param("org", organizationId).update();
    }

    public String findStatus(long organizationId, long alarmId, long policyId) {
        return jdbc.sql("SELECT status FROM data2flow_action.notification_escalations WHERE organization_id = :org AND alarm_id = :a AND policy_id = :p")
                .param("org", organizationId).param("a", alarmId).param("p", policyId).query(String.class).optional().orElse(null);
    }

    /** 대기 한 건 */
    public record Escalation(long id, long organizationId, long alarmId, long policyId, int nextStep, Instant dueAt, String request) {
    }

    static Escalation map(ResultSet rs, int n) throws SQLException {
        return new Escalation(rs.getLong("id"), rs.getLong("organization_id"), rs.getLong("alarm_id"), rs.getLong("policy_id"),
                rs.getInt("next_step"), Pg.instant(rs, "due_at"), rs.getString("request"));
    }
}
