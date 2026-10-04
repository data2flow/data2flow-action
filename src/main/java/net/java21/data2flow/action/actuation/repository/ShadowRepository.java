package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

/** 기기 상태 쌍({@code data2flow_action.device_shadows}, ACT-02.04) */
@Repository
public class ShadowRepository {

    private static final String COLUMNS = """
            device_id, organization_id, desired::text AS desired, desired_version, desired_updated_at, desired_source::text AS desired_source,
            reported::text AS reported, reported_version, reported_at, connectivity""";

    private final JdbcClient jdbc;

    public ShadowRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 행이 없으면 만들고 잠근다(같은 트랜잭션에서 상태를 바꾼다) */
    public ShadowRow lockOrCreate(long organizationId, long deviceId) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.device_shadows (device_id, organization_id) VALUES (:device, :org)
                        ON CONFLICT (device_id) DO NOTHING""")
                .param("device", deviceId).param("org", organizationId).update();
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.device_shadows WHERE device_id = :device AND organization_id = :org FOR UPDATE")
                .param("device", deviceId).param("org", organizationId).query(MAPPER).single();
    }

    public Optional<ShadowRow> findByDeviceIdAndOrganizationId(long deviceId, long organizationId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM data2flow_action.device_shadows WHERE device_id = :device AND organization_id = :org")
                .param("device", deviceId).param("org", organizationId).query(MAPPER).optional();
    }

    @OrganizationScopeExempt("잠근 행을 기기 ID(전역 고유)로 갱신")
    public void save(ShadowRow row, Instant now) {
        jdbc.sql("""
                        UPDATE data2flow_action.device_shadows
                           SET desired = CAST(:desired AS jsonb), desired_version = :desiredVersion, desired_updated_at = :desiredAt,
                               desired_source = CAST(:desiredSource AS jsonb), reported = CAST(:reported AS jsonb),
                               reported_version = :reportedVersion, reported_at = :reportedAt, delta = CAST(:delta AS jsonb),
                               connectivity = :connectivity, updated_at = :now
                         WHERE device_id = :device""")
                .param("desired", Json.write(row.shadow().desired())).param("desiredVersion", row.shadow().desiredVersion())
                .param("desiredAt", Pg.ts(row.desiredUpdatedAt()))
                .param("desiredSource", row.desiredSource() == null ? null : Json.write(row.desiredSource()))
                .param("reported", Json.write(row.shadow().reported())).param("reportedVersion", row.shadow().reportedVersion())
                .param("reportedAt", Pg.ts(row.shadow().reportedAt())).param("delta", Json.write(row.shadow().delta()))
                .param("connectivity", row.connectivity()).param("now", Pg.ts(now)).param("device", row.deviceId())
                .update();
    }

    /**
     * 업링크 신호(EVT-ACT-07 상태 없음): 상태 쌍이 있는 기기만 마지막 보고 시각을 앞으로 당긴다. 행을 만들지 않고(센서마다 행이 생기지 않게),
     * 더 늦은 시각만 반영해 다시 받은 신호·늦게 온 신호가 시각을 되돌리지 않는다(멱등).
     *
     * @return 바꾼 행 수(0 또는 1)
     */
    public int touchReportedAt(long organizationId, long deviceId, Instant at) {
        if (at == null) {
            return 0;
        }
        return jdbc.sql("""
                        UPDATE data2flow_action.device_shadows SET reported_at = :at
                         WHERE device_id = :device AND organization_id = :org AND (reported_at IS NULL OR reported_at < :at)""")
                .param("at", Pg.ts(at)).param("device", deviceId).param("org", organizationId).update();
    }

    /** 기기 삭제(EVT-DEV-01 DELETED): 상태 쌍·수동 우선·보호 상태를 지운다. 명령 이력은 보관 기간 동안 둔다 */
    public void deleteDevice(long organizationId, long deviceId) {
        for (String table : new String[]{"device_shadows", "manual_overrides", "protection_state"}) {
            jdbc.sql("DELETE FROM data2flow_action." + table + " WHERE organization_id = :org AND device_id = :device")
                    .param("org", organizationId).param("device", deviceId).update();
        }
    }

    /**
     * 상태 쌍 한 행.
     *
     * @param connectivity UNKNOWN·ONLINE·OFFLINE
     */
    public record ShadowRow(long deviceId, long organizationId, DeviceShadow shadow, Instant desiredUpdatedAt,
                            CommandSource desiredSource, String connectivity) {

        public ShadowRow withShadow(DeviceShadow s) {
            return new ShadowRow(deviceId, organizationId, s, desiredUpdatedAt, desiredSource, connectivity);
        }

        public ShadowRow withDesired(DeviceShadow s, Instant at, CommandSource source) {
            return new ShadowRow(deviceId, organizationId, s, at, source, connectivity);
        }

        public ShadowRow withConnectivity(String c) {
            return new ShadowRow(deviceId, organizationId, shadow, desiredUpdatedAt, desiredSource, c);
        }

        public boolean offline() {
            return "OFFLINE".equals(connectivity);
        }
    }

    static final RowMapper<ShadowRow> MAPPER = (rs, n) -> new ShadowRow(rs.getLong("device_id"), rs.getLong("organization_id"),
            new DeviceShadow(Json.state(rs.getString("desired")), rs.getLong("desired_version"), Json.state(rs.getString("reported")),
                    rs.getLong("reported_version"), Pg.instant(rs, "reported_at")),
            Pg.instant(rs, "desired_updated_at"), Json.read(rs.getString("desired_source"), CommandSource.class),
            rs.getString("connectivity"));
}
