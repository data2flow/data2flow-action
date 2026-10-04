package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** 제어 효과 확인 대기({@code effect_checks}, ACT-08.01) */
@Repository
public class EffectCheckRepository {

    private final JdbcClient jdbc;

    public EffectCheckRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Check c, String env) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.effect_checks (command_id, organization_id, device_id, space_id, capability, metric, direction,
                            within_minutes, start_at, due_at, env)
                        VALUES (:id, :org, :device, :space, :capability, :metric, :direction, :within, :start, :due, :env)
                        ON CONFLICT (command_id) DO NOTHING""")
                .param("id", c.commandId()).param("org", c.organizationId()).param("device", c.deviceId()).param("space", c.spaceId())
                .param("capability", c.capability()).param("metric", c.metric()).param("direction", c.direction())
                .param("within", c.withinMinutes()).param("start", Pg.ts(c.startAt())).param("due", Pg.ts(c.dueAt())).param("env", env)
                .update();
    }

    /** 기한이 된 확인(잠그지 않음: 값 조회는 트랜잭션 밖) */
    @OrganizationScopeExempt("효과 확인 작업은 이 배포의 모든 조직을 처리한다")
    public List<Check> findDue(Instant now, int limit, String env) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.effect_checks WHERE env = :env AND status = 'PENDING' AND due_at <= :now"
                        + " ORDER BY due_at LIMIT :limit")
                .param("env", env).param("now", Pg.ts(now)).param("limit", limit).query(this::map).list();
    }

    /** 아직 PENDING이면 잠근다(다른 파드가 이미 끝냈으면 빈 값) */
    @OrganizationScopeExempt("명령 ID(전역 고유 UUID)로 잠근다")
    public Optional<Check> lockPending(UUID commandId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.effect_checks WHERE command_id = :id AND status = 'PENDING' FOR UPDATE SKIP LOCKED")
                .param("id", commandId).query(this::map).optional();
    }

    @OrganizationScopeExempt("잠근 행을 명령 ID로 갱신")
    public void finish(UUID commandId, String status, Double start, Double end, boolean eventEmitted, Instant now) {
        jdbc.sql("""
                        UPDATE data2flow_action.effect_checks SET status = :status, start_value = :start, end_value = :end,
                               event_emitted = :emitted, finished_at = :now WHERE command_id = :id""")
                .param("status", status).param("start", start).param("end", end).param("emitted", eventEmitted).param("now", Pg.ts(now))
                .param("id", commandId).update();
    }

    /** 같은 기기에서 이 시각 뒤에 낸 효과 없음 이벤트가 있는가(BR-ACT-20: 1시간에 1회) */
    @OrganizationScopeExempt("기기 ID(전역 고유)로 센다")
    public boolean emittedSince(long deviceId, Instant since) {
        return jdbc.sql("""
                        SELECT count(*) FROM data2flow_action.effect_checks
                         WHERE device_id = :device AND status = 'NO_EFFECT' AND event_emitted AND finished_at > :since""")
                .param("device", deviceId).param("since", Pg.ts(since)).query(Long.class).single() > 0;
    }

    /** 기기의 효과 없음 기록(API-ACT-35 noEffectEvents) */
    public List<Check> findNoEffect(long organizationId, long deviceId, Instant from, Instant to) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.effect_checks WHERE organization_id = :org AND device_id = :device"
                        + " AND status = 'NO_EFFECT' AND finished_at >= :from AND finished_at < :to ORDER BY finished_at")
                .param("org", organizationId).param("device", deviceId).param("from", Pg.ts(from)).param("to", Pg.ts(to))
                .query(this::map).list();
    }

    private static final String COLUMNS = """
            command_id, organization_id, device_id, space_id, capability, metric, direction, within_minutes, start_at, due_at, status,
            start_value, end_value, finished_at""";

    private Check map(java.sql.ResultSet rs, int n) throws java.sql.SQLException {
        return new Check(rs.getObject("command_id", UUID.class), rs.getLong("organization_id"), rs.getLong("device_id"),
                Pg.longOrNull(rs, "space_id"), rs.getString("capability"), rs.getString("metric"), rs.getString("direction"),
                rs.getInt("within_minutes"), Pg.instant(rs, "start_at"), Pg.instant(rs, "due_at"), rs.getString("status"),
                (Double) rs.getObject("start_value"), (Double) rs.getObject("end_value"), Pg.instant(rs, "finished_at"));
    }

    public record Check(UUID commandId, long organizationId, long deviceId, Long spaceId, String capability, String metric, String direction,
                        int withinMinutes, Instant startAt, Instant dueAt, String status, Double startValue, Double endValue,
                        Instant finishedAt) {
    }
}
