package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** 드라이버 호출 기록({@code driver_calls}, ACT-03.06)과 서킷 상태({@code driver_circuits}, BR-ACT-14) */
@Repository
public class DriverCallRepository {

    private final JdbcClient jdbc;

    public DriverCallRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(long organizationId, long driverId, String driverType, Instant at, boolean ok, long latencyMs, UUID commandId,
                       String error) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.driver_calls (organization_id, driver_id, driver_type, at, ok, latency_ms, command_id, error)
                        VALUES (:org, :driver, :type, :at, :ok, :ms, :command, :error)""")
                .param("org", organizationId).param("driver", driverId).param("type", driverType).param("at", Pg.ts(at)).param("ok", ok)
                .param("ms", (int) Math.min(Integer.MAX_VALUE, latencyMs)).param("command", commandId)
                .param("error", error == null ? null : error.substring(0, Math.min(500, error.length())))
                .update();
    }

    /** 창 안 (호출 수, 실패 수) */
    @OrganizationScopeExempt("서킷 판정: 드라이버 ID(전역 고유)로 센다")
    public int[] countSince(long driverId, Instant since) {
        return jdbc.sql("SELECT count(*) AS n, count(*) FILTER (WHERE NOT ok) AS f FROM data2flow_action.driver_calls WHERE driver_id = :d AND at > :since")
                .param("d", driverId).param("since", Pg.ts(since))
                .query((rs, n) -> new int[]{rs.getInt("n"), rs.getInt("f")}).single();
    }

    /** 지표(API-ACT-32): 호출 수, 실패 수, 평균·p95 응답 시간 */
    public Stats stats(long organizationId, long driverId, Instant since, Instant until) {
        return jdbc.sql("""
                        SELECT count(*) AS n, count(*) FILTER (WHERE NOT ok) AS f, avg(latency_ms) AS avg_ms,
                               percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms
                          FROM data2flow_action.driver_calls
                         WHERE organization_id = :org AND driver_id = :d AND at > :since AND at <= :until""")
                .param("org", organizationId).param("d", driverId).param("since", Pg.ts(since)).param("until", Pg.ts(until))
                .query((rs, n) -> new Stats(rs.getInt("n"), rs.getInt("f"), rs.getDouble("avg_ms"), rs.getDouble("p95_ms"))).single();
    }

    /** 최근 오류 */
    public List<RecentError> findRecentErrors(long organizationId, long driverId, Instant since, int limit) {
        return jdbc.sql("""
                        SELECT at, command_id, error FROM data2flow_action.driver_calls
                         WHERE organization_id = :org AND driver_id = :d AND at > :since AND NOT ok ORDER BY at DESC LIMIT :limit""")
                .param("org", organizationId).param("d", driverId).param("since", Pg.ts(since)).param("limit", limit)
                .query((rs, n) -> new RecentError(Pg.instant(rs, "at"), rs.getObject("command_id", UUID.class), rs.getString("error")))
                .list();
    }

    @OrganizationScopeExempt("보관 정리: 모든 조직의 오래된 호출 기록")
    public int deleteBefore(Instant before) {
        return jdbc.sql("DELETE FROM data2flow_action.driver_calls WHERE at < :before").param("before", Pg.ts(before)).update();
    }

    // ───────────── 서킷 ─────────────

    /** 서킷 상태를 잠근다(없으면 CLOSED로 만든다) */
    @OrganizationScopeExempt("드라이버 ID(전역 고유)로 잠근다")
    public DriverCircuitBreaker.Circuit lockCircuit(long driverId, long organizationId, String driverType, Instant now) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.driver_circuits (driver_id, organization_id, driver_type, state, updated_at)
                        VALUES (:d, :org, :type, 'CLOSED', :now) ON CONFLICT (driver_id) DO NOTHING""")
                .param("d", driverId).param("org", organizationId).param("type", driverType).param("now", Pg.ts(now)).update();
        return jdbc.sql("SELECT state, opened_at, trial_started_at FROM data2flow_action.driver_circuits WHERE driver_id = :d FOR UPDATE")
                .param("d", driverId)
                .query((rs, n) -> new DriverCircuitBreaker.Circuit(DriverCircuitBreaker.State.valueOf(rs.getString("state")),
                        Pg.instant(rs, "opened_at"), Pg.instant(rs, "trial_started_at")))
                .single();
    }

    @OrganizationScopeExempt("잠근 행을 드라이버 ID(전역 고유)로 갱신")
    public void saveCircuit(long driverId, DriverCircuitBreaker.Circuit c, Double failureRate, Instant now) {
        jdbc.sql("""
                        UPDATE data2flow_action.driver_circuits
                           SET state = :state, opened_at = :opened, trial_started_at = :trial,
                               failure_rate = coalesce(CAST(:rate AS double precision), failure_rate), updated_at = :now
                         WHERE driver_id = :d""")
                .param("state", c.state().name()).param("opened", Pg.ts(c.openedAt())).param("trial", Pg.ts(c.trialStartedAt()))
                .param("rate", failureRate).param("now", Pg.ts(now)).param("d", driverId)
                .update();
    }

    public Optional<CircuitRow> findCircuit(long organizationId, long driverId) {
        return jdbc.sql("""
                        SELECT state, opened_at, failure_rate FROM data2flow_action.driver_circuits
                         WHERE organization_id = :org AND driver_id = :d""")
                .param("org", organizationId).param("d", driverId)
                .query((rs, n) -> new CircuitRow(rs.getString("state"), Pg.instant(rs, "opened_at"), rs.getDouble("failure_rate")))
                .optional();
    }

    public record Stats(int requests, int errors, double avgMs, double p95Ms) {
    }

    public record RecentError(Instant at, UUID commandId, String message) {
    }

    public record CircuitRow(String state, Instant openedAt, double failureRate) {
    }
}
