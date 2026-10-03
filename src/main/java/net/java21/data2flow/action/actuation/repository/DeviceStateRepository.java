package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.actuation.domain.ProtectionState;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 기기별 부가 상태: 수동 우선({@code manual_overrides}, BR-ACT-08), 보호 상태({@code protection_state}, BR-ACT-10),
 * 액추에이터 상태 구간({@code device_state_history}, TSD-01.03).
 */
@Repository
public class DeviceStateRepository {

    private final JdbcClient jdbc;

    public DeviceStateRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    // ───────────── 수동 우선 ─────────────

    public Optional<ManualOverride> findManualOverride(long organizationId, long deviceId, String capability) {
        return jdbc.sql("""
                        SELECT capability, until, set_by FROM data2flow_action.manual_overrides
                         WHERE organization_id = :org AND device_id = :device AND capability = :capability""")
                .param("org", organizationId).param("device", deviceId).param("capability", capability)
                .query((rs, n) -> new ManualOverride(rs.getString("capability"), Pg.instant(rs, "until"), rs.getLong("set_by")))
                .optional();
    }

    public java.util.List<ManualOverride> findManualOverrides(long organizationId, long deviceId, Instant now) {
        return jdbc.sql("""
                        SELECT capability, until, set_by FROM data2flow_action.manual_overrides
                         WHERE organization_id = :org AND device_id = :device AND until > :now ORDER BY capability""")
                .param("org", organizationId).param("device", deviceId).param("now", Pg.ts(now))
                .query((rs, n) -> new ManualOverride(rs.getString("capability"), Pg.instant(rs, "until"), rs.getLong("set_by")))
                .list();
    }

    public void upsertManualOverride(long organizationId, long deviceId, String capability, Instant until, long setBy, UUID commandId,
                                     Instant now) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.manual_overrides (device_id, capability, organization_id, until, set_by, command_id, updated_at)
                        VALUES (:device, :capability, :org, :until, :setBy, :command, :now)
                        ON CONFLICT (device_id, capability) DO UPDATE
                           SET until = EXCLUDED.until, set_by = EXCLUDED.set_by, command_id = EXCLUDED.command_id, updated_at = EXCLUDED.updated_at""")
                .param("device", deviceId).param("capability", capability).param("org", organizationId).param("until", Pg.ts(until))
                .param("setBy", setBy).param("command", commandId).param("now", Pg.ts(now))
                .update();
    }

    /** 수동 우선 해제(API-ACT-06). capability가 null이면 그 기기 전체. 지운 행 수 */
    public int deleteManualOverride(long organizationId, long deviceId, String capability) {
        return jdbc.sql("""
                        DELETE FROM data2flow_action.manual_overrides
                         WHERE organization_id = :org AND device_id = :device AND (CAST(:capability AS varchar) IS NULL OR capability = :capability)""")
                .param("org", organizationId).param("device", deviceId).param("capability", capability)
                .update();
    }

    // ───────────── 보호 상태 ─────────────

    public ProtectionState findProtection(long organizationId, long deviceId) {
        return jdbc.sql("""
                        SELECT last_on_at, last_off_at, cycles_today, cycles_date FROM data2flow_action.protection_state
                         WHERE organization_id = :org AND device_id = :device""")
                .param("org", organizationId).param("device", deviceId)
                .query((rs, n) -> new ProtectionState(Pg.instant(rs, "last_on_at"), Pg.instant(rs, "last_off_at"),
                        rs.getInt("cycles_today"), rs.getObject("cycles_date", LocalDate.class)))
                .optional().orElse(ProtectionState.EMPTY);
    }

    public void saveProtection(long organizationId, long deviceId, ProtectionState s, Instant now) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.protection_state (device_id, organization_id, last_on_at, last_off_at, cycles_today, cycles_date, updated_at)
                        VALUES (:device, :org, :on, :off, :cycles, :date, :now)
                        ON CONFLICT (device_id) DO UPDATE
                           SET last_on_at = EXCLUDED.last_on_at, last_off_at = EXCLUDED.last_off_at, cycles_today = EXCLUDED.cycles_today,
                               cycles_date = EXCLUDED.cycles_date, updated_at = EXCLUDED.updated_at""")
                .param("device", deviceId).param("org", organizationId).param("on", Pg.ts(s.lastOnAt())).param("off", Pg.ts(s.lastOffAt()))
                .param("cycles", s.cyclesToday()).param("date", s.cyclesDate()).param("now", Pg.ts(now))
                .update();
    }

    // ───────────── 상태 구간(TSD-01.03) ─────────────

    /** 속성 값이 바뀌면 열린 구간을 닫고 새 구간을 연다. 열린 구간은 기기·기능·속성마다 하나(애플리케이션이 보장) */
    public void recordState(long organizationId, long deviceId, String capability, String attribute, Object value, Instant at,
                            Map<String, Object> source, UUID commandId) {
        jdbc.sql("""
                        UPDATE data2flow_action.device_state_history SET valid_to = :at
                         WHERE organization_id = :org AND device_id = :device AND capability = :capability AND attribute = :attribute
                           AND valid_to IS NULL AND valid_from <= :at""")
                .param("at", Pg.ts(at)).param("org", organizationId).param("device", deviceId).param("capability", capability)
                .param("attribute", attribute).update();
        if (value == null) {
            return;
        }
        jdbc.sql("""
                        INSERT INTO data2flow_action.device_state_history (organization_id, device_id, capability, attribute, value, valid_from, source, command_id)
                        VALUES (:org, :device, :capability, :attribute, CAST(:value AS jsonb), :at, CAST(:source AS jsonb), :command)""")
                .param("org", organizationId).param("device", deviceId).param("capability", capability).param("attribute", attribute)
                .param("value", Json.write(value)).param("at", Pg.ts(at)).param("source", source == null ? null : Json.write(source))
                .param("command", commandId)
                .update();
    }

    /** 다음 달 월 파티션을 미리 만든다(ERD: 월 RANGE 파티션 + DEFAULT, 스케줄러가 생성) */
    public void ensureMonthPartition(LocalDate monthStart) {
        LocalDate next = monthStart.plusMonths(1);
        String name = "device_state_history_y%04dm%02d".formatted(monthStart.getYear(), monthStart.getMonthValue());
        jdbc.sql("CREATE TABLE IF NOT EXISTS data2flow_action." + name + " PARTITION OF data2flow_action.device_state_history"
                + " FOR VALUES FROM ('" + monthStart + " 00:00:00+00') TO ('" + next + " 00:00:00+00')").update();
    }

    /**
     * @param capability 기능
     * @param until      이 시각까지 자동 명령 SKIPPED(MANUAL_OVERRIDE)
     * @param setBy      수동 명령을 낸 사용자
     */
    public record ManualOverride(String capability, Instant until, long setBy) {
    }
}
