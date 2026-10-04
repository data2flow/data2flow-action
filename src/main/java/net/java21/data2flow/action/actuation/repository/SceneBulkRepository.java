package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 일괄 제어 작업({@code bulk_jobs}, ACT-02.06)과 장면 실행({@code scene_runs}, ACT-05.01) */
@Repository
public class SceneBulkRepository {

    private final JdbcClient jdbc;

    public SceneBulkRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // ───────────── 일괄 ─────────────

    public long insertBulkJob(long organizationId, long requestedBy, Map<String, Object> target, String capability, String command,
                              Map<String, Object> args, int total, Instant now) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.bulk_jobs (organization_id, requested_by, target, capability, command, args, total, created_at)
                        VALUES (:org, :by, CAST(:target AS jsonb), :capability, :command, CAST(:args AS jsonb), :total, :now) RETURNING id""")
                .param("org", organizationId).param("by", requestedBy).param("target", Json.write(target)).param("capability", capability)
                .param("command", command).param("args", Json.write(args)).param("total", total).param("now", Pg.ts(now))
                .query(Long.class).single();
    }

    public Optional<BulkJob> findBulkJob(long organizationId, long id) {
        return jdbc.sql("""
                        SELECT id, organization_id, requested_by, target::text AS target, capability, command, args::text AS args, total, status, created_at
                          FROM data2flow_action.bulk_jobs WHERE organization_id = :org AND id = :id""")
                .param("org", organizationId).param("id", id).query(this::bulk).optional();
    }

    /** 끝나지 않은 오래된 작업(파드가 실행 중 죽었을 때 이어서 제출) */
    @OrganizationScopeExempt("이어 하기 작업은 모든 조직의 멈춘 일괄 작업을 본다")
    public List<BulkJob> findStaleRunning(Instant before) {
        return jdbc.sql("""
                        SELECT id, organization_id, requested_by, target::text AS target, capability, command, args::text AS args, total, status, created_at
                          FROM data2flow_action.bulk_jobs WHERE status = 'RUNNING' AND created_at < :before ORDER BY created_at LIMIT 20""")
                .param("before", Pg.ts(before)).query(this::bulk).list();
    }

    public void finishBulkJob(long organizationId, long id, int succeeded, int failed, int queued, Instant now) {
        jdbc.sql("""
                        UPDATE data2flow_action.bulk_jobs SET status = 'COMPLETED', succeeded = :ok, failed = :failed, queued = :queued, finished_at = :now
                         WHERE organization_id = :org AND id = :id""")
                .param("ok", succeeded).param("failed", failed).param("queued", queued).param("now", Pg.ts(now))
                .param("org", organizationId).param("id", id).update();
    }

    private BulkJob bulk(java.sql.ResultSet rs, int n) throws java.sql.SQLException {
        return new BulkJob(rs.getLong("id"), rs.getLong("organization_id"), rs.getLong("requested_by"), Json.map(rs.getString("target")),
                rs.getString("capability"), rs.getString("command"), Json.map(rs.getString("args")), rs.getInt("total"), rs.getString("status"),
                Pg.instant(rs, "created_at"));
    }

    // ───────────── 장면 ─────────────

    public long insertSceneRun(long organizationId, long sceneId, Map<String, Object> source, Instant now) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.scene_runs (organization_id, scene_id, source, started_at)
                        VALUES (:org, :scene, CAST(:source AS jsonb), :now) RETURNING id""")
                .param("org", organizationId).param("scene", sceneId).param("source", Json.write(source)).param("now", Pg.ts(now))
                .query(Long.class).single();
    }

    public void saveSceneRun(long organizationId, long id, String status, List<Map<String, Object>> results, Instant finishedAt) {
        jdbc.sql("""
                        UPDATE data2flow_action.scene_runs SET status = :status, results = CAST(:results AS jsonb), finished_at = :finished
                         WHERE organization_id = :org AND id = :id""")
                .param("status", status).param("results", Json.write(results)).param("finished", Pg.ts(finishedAt))
                .param("org", organizationId).param("id", id).update();
    }

    public Optional<SceneRun> findSceneRun(long organizationId, long id) {
        return jdbc.sql("""
                        SELECT id, scene_id, source::text AS source, status, results::text AS results, started_at, finished_at
                          FROM data2flow_action.scene_runs WHERE organization_id = :org AND id = :id""")
                .param("org", organizationId).param("id", id)
                .query((rs, n) -> new SceneRun(rs.getLong("id"), rs.getLong("scene_id"), Json.map(rs.getString("source")), rs.getString("status"),
                        Json.read(rs.getString("results"), List.class), Pg.instant(rs, "started_at"), Pg.instant(rs, "finished_at")))
                .optional();
    }

    public record BulkJob(long id, long organizationId, long requestedBy, Map<String, Object> target, String capability, String command,
                          Map<String, Object> args, int total, String status, Instant createdAt) {
    }

    public record SceneRun(long id, long sceneId, Map<String, Object> source, String status, List<?> results, Instant startedAt,
                           Instant finishedAt) {
    }
}
