package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** 명령 타임라인({@code data2flow_action.command_events}, ERD 규칙 §11.5) */
@Repository
public class CommandEventRepository {

    private final JdbcClient jdbc;

    public CommandEventRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 전이 한 건을 남기고 행 ID를 돌려준다 */
    public long insert(UUID commandId, long organizationId, Instant at, String from, String to, String reason, Map<String, Object> detail) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.command_events (command_id, organization_id, at, from_status, to_status, reason, detail)
                        VALUES (:command, :org, :at, :from, :to, :reason, CAST(:detail AS jsonb)) RETURNING id""")
                .param("command", commandId).param("org", organizationId).param("at", Pg.ts(at)).param("from", from).param("to", to)
                .param("reason", reason).param("detail", detail == null || detail.isEmpty() ? null : Json.write(detail))
                .query(Long.class).single();
    }

    public List<TimelineEntry> findByCommand(long organizationId, UUID commandId) {
        return jdbc.sql("""
                        SELECT to_status, at, reason FROM data2flow_action.command_events
                         WHERE organization_id = :org AND command_id = :command ORDER BY at, id""")
                .param("org", organizationId).param("command", commandId)
                .query((rs, n) -> new TimelineEntry(rs.getString("to_status"), Pg.instant(rs, "at"), rs.getString("reason")))
                .list();
    }

    /** 타임라인 한 줄(API-ACT-01 {@code timeline[{status, at, reason?}]}) */
    public record TimelineEntry(String status, Instant at, String reason) {
    }
}
