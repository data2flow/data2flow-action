package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** 명령({@code data2flow_action.commands}). 명령 ID(UUID)는 전역 고유라 이벤트 처리 경로는 ID만으로 찾는다 */
@Repository
public class CommandRepository {

    /** 같은 기능의 대기 명령(대체 대상): 아직 드라이버로 가지 않은 상태 */
    public static final List<String> PENDING = List.of("DELAYED", "QUEUED", "QUEUED_FOR_DOWNLINK");
    /** 적용 판정 대상: 드라이버로 갔거나 가는 중 */
    public static final List<String> IN_FLIGHT = List.of("REQUESTED", "SENT", "ACKED");
    /** 최소 간격·진동 판정에서 "보낸 명령"으로 보지 않는 상태 */
    static final List<String> NOT_SENT = List.of("REJECTED", "BLOCKED", "SKIPPED", "SUPERSEDED", "CANCELLED");

    private static final String COLUMNS = """
            id, organization_id, idempotency_key, device_id, capability, command, args::text AS args, priority, source::text AS source,
            status, status_reason, valid_until, execute_after, attempts, requested_at, sent_at, acked_at, applied_at, finished_at, timeout_at""";

    private final JdbcClient jdbc;

    public CommandRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 새 명령. 같은 (조직, 멱등 키)가 있으면 넣지 않고 false */
    public boolean insert(Command c, String env) {
        return jdbc.sql("""
                        INSERT INTO data2flow_action.commands (id, organization_id, idempotency_key, device_id, capability, command, args,
                            priority, source, status, status_reason, valid_until, execute_after, attempts, requested_at, timeout_at, env)
                        VALUES (:id, :org, :key, :device, :capability, :command, CAST(:args AS jsonb), :priority, CAST(:source AS jsonb),
                            :status, :reason, :validUntil, :executeAfter, :attempts, :requestedAt, :timeoutAt, :env)
                        ON CONFLICT (organization_id, idempotency_key) DO NOTHING""")
                .param("id", c.id()).param("org", c.organizationId()).param("key", c.idempotencyKey())
                .param("device", c.deviceId()).param("capability", c.capability()).param("command", c.command())
                .param("args", Json.write(c.args())).param("priority", c.priority().name()).param("source", Json.write(c.source()))
                .param("status", c.status().name()).param("reason", c.statusReason())
                .param("validUntil", Pg.ts(c.validUntil())).param("executeAfter", Pg.ts(c.executeAfter()))
                .param("attempts", c.attempts()).param("requestedAt", Pg.ts(c.requestedAt())).param("timeoutAt", Pg.ts(c.timeoutAt()))
                .param("env", env)
                .update() == 1;
    }

    /** 상태와 시각들을 저장한다(호출 쪽이 같은 트랜잭션에서 {@link #lock}으로 잠근 행) */
    @OrganizationScopeExempt("잠근 행을 명령 ID(전역 고유 UUID)로 갱신")
    public void save(Command c) {
        jdbc.sql("""
                        UPDATE data2flow_action.commands
                           SET status = :status, status_reason = :reason, execute_after = :executeAfter, attempts = :attempts,
                               sent_at = :sentAt, acked_at = :ackedAt, applied_at = :appliedAt, finished_at = :finishedAt, timeout_at = :timeoutAt
                         WHERE id = :id""")
                .param("status", c.status().name()).param("reason", c.statusReason()).param("executeAfter", Pg.ts(c.executeAfter()))
                .param("attempts", c.attempts()).param("sentAt", Pg.ts(c.sentAt())).param("ackedAt", Pg.ts(c.ackedAt()))
                .param("appliedAt", Pg.ts(c.appliedAt())).param("finishedAt", Pg.ts(c.finishedAt())).param("timeoutAt", Pg.ts(c.timeoutAt()))
                .param("id", c.id())
                .update();
    }

    /** 드라이버 응답 요약 */
    @OrganizationScopeExempt("명령 ID(전역 고유 UUID)로 갱신")
    public void saveDriverResponse(UUID id, String json) {
        jdbc.sql("UPDATE data2flow_action.commands SET driver_response = CAST(:r AS jsonb) WHERE id = :id")
                .param("r", json).param("id", id).update();
    }

    public Optional<Command> findByKey(long organizationId, String idempotencyKey) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE organization_id = :org AND idempotency_key = :key")
                .param("org", organizationId).param("key", idempotencyKey).query(MAPPER).optional();
    }

    public List<Command> findByKeys(long organizationId, Collection<String> keys) {
        if (keys.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE organization_id = :org AND idempotency_key IN (:keys)")
                .param("org", organizationId).param("keys", keys).query(MAPPER).list();
    }

    public Optional<Command> findByIdAndOrganizationId(UUID id, long organizationId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE id = :id AND organization_id = :org")
                .param("id", id).param("org", organizationId).query(MAPPER).optional();
    }

    /** 행을 잠근다(같은 트랜잭션 안에서 상태 전이) */
    @OrganizationScopeExempt("ack·상태 보고·기한 처리: 명령 ID(전역 고유 UUID)로 잠근다")
    public Optional<Command> lock(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE id = :id FOR UPDATE")
                .param("id", id).query(MAPPER).optional();
    }

    /** 기기의 진행 중 명령(적용 판정), 잠금 */
    @OrganizationScopeExempt("상태 보고 처리: 기기 ID(전역 고유)로 찾는다")
    public List<Command> lockInFlight(long deviceId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE device_id = :device AND status IN (:statuses)"
                        + " ORDER BY requested_at, id FOR UPDATE")
                .param("device", deviceId).param("statuses", IN_FLIGHT).query(MAPPER).list();
    }

    /** 같은 기기·기능의 대기 명령(새 명령이 대체, BR-ACT-13), 잠금 */
    @OrganizationScopeExempt("기기 ID(전역 고유)로 찾는다")
    public List<Command> lockPending(long deviceId, String capability, UUID except) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE device_id = :device AND capability = :capability"
                        + " AND status IN (:statuses) AND id <> :except ORDER BY requested_at FOR UPDATE")
                .param("device", deviceId).param("capability", capability).param("statuses", PENDING).param("except", except)
                .query(MAPPER).list();
    }

    /** 기기의 오프라인 대기 명령(재연결 시 전송) */
    @OrganizationScopeExempt("재연결 처리: 기기 ID(전역 고유)로 찾는다")
    public List<Command> lockQueued(long deviceId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE device_id = :device AND status = 'QUEUED'"
                        + " ORDER BY requested_at FOR UPDATE")
                .param("device", deviceId).query(MAPPER).list();
    }

    /** 같은 기기·기능에서 이 명령 전에 보낸 명령들(최소 간격·진동 판정), 오래된 것부터 */
    @OrganizationScopeExempt("기기 ID(전역 고유)로 찾는다")
    public List<Command> findSentSince(long deviceId, String capability, Instant since, UUID except) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE device_id = :device AND capability = :capability"
                        + " AND requested_at >= :since AND id <> :except AND status NOT IN (:notSent) ORDER BY requested_at, id")
                .param("device", deviceId).param("capability", capability).param("since", Pg.ts(since)).param("except", except)
                .param("notSent", NOT_SENT).query(MAPPER).list();
    }

    /** 같은 기기·기능의 직전 "보낸" 명령 요청 시각 */
    @OrganizationScopeExempt("기기 ID(전역 고유)로 찾는다")
    public Optional<Instant> lastSentAt(long deviceId, String capability, UUID except) {
        return jdbc.sql("SELECT max(requested_at) AS t FROM data2flow_action.commands WHERE device_id = :device AND capability = :capability"
                        + " AND id <> :except AND status NOT IN (:notSent)")
                .param("device", deviceId).param("capability", capability).param("except", except).param("notSent", NOT_SENT)
                .query((rs, n) -> Optional.ofNullable(Pg.instant(rs, "t"))).single();
    }

    /** 기기의 진행 중·대기 명령 요약(API-ACT-03 pending) */
    public List<Command> findOpenByDevice(long organizationId, long deviceId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE organization_id = :org AND device_id = :device"
                        + " AND status IN ('REQUESTED','DELAYED','QUEUED','QUEUED_FOR_DOWNLINK','SENT','ACKED') ORDER BY requested_at")
                .param("org", organizationId).param("device", deviceId).query(MAPPER).list();
    }

    /**
     * 기한이 된 명령(기한 작업). 다른 파드가 잠근 행은 건너뛴다. 자기 배포({@code env}) 행만 본다.
     */
    @OrganizationScopeExempt("기한 작업은 이 배포의 모든 조직 명령을 처리한다")
    public List<Due> lockDue(Instant now, int limit, String env) {
        return jdbc.sql("""
                        SELECT id, status FROM data2flow_action.commands
                         WHERE env = :env
                           AND ((status IN ('REQUESTED','QUEUED','SENT','ACKED') AND timeout_at <= :now)
                             OR (status = 'DELAYED' AND execute_after <= :now))
                         ORDER BY coalesce(timeout_at, execute_after)
                         LIMIT :limit
                         FOR UPDATE SKIP LOCKED""")
                .param("env", env).param("now", Pg.ts(now)).param("limit", limit)
                .query((rs, n) -> new Due(rs.getObject("id", UUID.class), CommandStatus.valueOf(rs.getString("status"))))
                .list();
    }

    /** 기기별 명령 이력 커서 목록(API-ACT-02, 최신순). 커서는 (requested_at, id) */
    public List<Command> listByDevice(CommandQuery q, Instant cursorAt, UUID cursorId, int fetchSize) {
        StringBuilder sql = new StringBuilder("SELECT " + COLUMNS + " FROM data2flow_action.commands WHERE organization_id = :org");
        if (q.deviceId() != null) {
            sql.append(" AND device_id = :device");
        }
        if (q.from() != null) {
            sql.append(" AND requested_at >= :from");
        }
        if (q.to() != null) {
            sql.append(" AND requested_at < :to");
        }
        if (q.sourceType() != null) {
            sql.append(" AND source_type = :sourceType");
        }
        if (q.status() != null) {
            sql.append(" AND status = :status");
        }
        if (q.capability() != null) {
            sql.append(" AND capability = :capability");
        }
        if (cursorAt != null) {
            sql.append(" AND (requested_at, id) < (:cursorAt, :cursorId)");
        }
        sql.append(" ORDER BY requested_at DESC, id DESC LIMIT :limit");
        var spec = jdbc.sql(sql.toString()).param("org", q.organizationId()).param("limit", fetchSize);
        if (q.deviceId() != null) {
            spec = spec.param("device", q.deviceId());
        }
        if (q.from() != null) {
            spec = spec.param("from", Pg.ts(q.from()));
        }
        if (q.to() != null) {
            spec = spec.param("to", Pg.ts(q.to()));
        }
        if (q.sourceType() != null) {
            spec = spec.param("sourceType", q.sourceType());
        }
        if (q.status() != null) {
            spec = spec.param("status", q.status());
        }
        if (q.capability() != null) {
            spec = spec.param("capability", q.capability());
        }
        if (cursorAt != null) {
            spec = spec.param("cursorAt", Pg.ts(cursorAt)).param("cursorId", cursorId);
        }
        return spec.query(MAPPER).list();
    }

    /** 기한이 된 명령 */
    public record Due(UUID id, CommandStatus status) {
    }

    /**
     * 이력 조건(API-ACT-02 {@code from, to, sourceType, status, capability}).
     */
    public record CommandQuery(long organizationId, Long deviceId, Instant from, Instant to, String sourceType, String status,
                               String capability) {
    }

    static final RowMapper<Command> MAPPER = (rs, n) -> new Command(
            rs.getObject("id", UUID.class), rs.getLong("organization_id"), rs.getString("idempotency_key").trim(),
            rs.getLong("device_id"), rs.getString("capability"), rs.getString("command"), Json.map(rs.getString("args")),
            CommandPriority.valueOf(rs.getString("priority")), Json.read(rs.getString("source"), CommandSource.class),
            CommandStatus.valueOf(rs.getString("status")), rs.getString("status_reason"), Pg.instant(rs, "valid_until"),
            Pg.instant(rs, "execute_after"), rs.getInt("attempts"), Pg.instant(rs, "requested_at"), Pg.instant(rs, "sent_at"),
            Pg.instant(rs, "acked_at"), Pg.instant(rs, "applied_at"), Pg.instant(rs, "finished_at"), Pg.instant(rs, "timeout_at"));
}
