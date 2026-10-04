package net.java21.data2flow.action.actuation.repository;

import net.java21.data2flow.action.actuation.domain.RuntimeAggregator;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.common.Pg;
import net.java21.data2flow.contracts.tenancy.OrganizationScopeExempt;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/** 일별 가동 집계({@code runtime_stat_daily}, ACT-08.02)와 그 원천인 전원 상태 구간 */
@Repository
public class RuntimeStatRepository {

    private final JdbcClient jdbc;

    public RuntimeStatRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 이 기간에 상태 구간이 있는 기기(집계 대상) */
    @OrganizationScopeExempt("집계 작업은 모든 조직의 기기를 본다")
    public List<long[]> findDevicesWithHistory(Instant from, Instant to) {
        return jdbc.sql("""
                        SELECT DISTINCT organization_id, device_id FROM data2flow_action.device_state_history
                         WHERE valid_from < :to AND (valid_to IS NULL OR valid_to > :from)""")
                .param("from", Pg.ts(from)).param("to", Pg.ts(to))
                .query((rs, n) -> new long[]{rs.getLong("organization_id"), rs.getLong("device_id")}).list();
    }

    /** 전원을 정하는 속성(Switch.on, Thermostat·Ventilation.mode)의 구간들(기간과 겹치는 것, 시작 순) */
    public List<StateRow> findPowerHistory(long organizationId, long deviceId, Instant from, Instant to) {
        return jdbc.sql("""
                        SELECT capability, attribute, value::text AS value, valid_from, valid_to FROM data2flow_action.device_state_history
                         WHERE organization_id = :org AND device_id = :device
                           AND ((capability = 'Switch' AND attribute = 'on') OR (capability IN ('Thermostat','Ventilation') AND attribute = 'mode'))
                           AND valid_from < :to AND (valid_to IS NULL OR valid_to > :from)
                         ORDER BY valid_from""")
                .param("org", organizationId).param("device", deviceId).param("from", Pg.ts(from)).param("to", Pg.ts(to))
                .query((rs, n) -> new StateRow(rs.getString("capability"), rs.getString("attribute"), Json.read(rs.getString("value"), Object.class),
                        Pg.instant(rs, "valid_from"), Pg.instant(rs, "valid_to")))
                .list();
    }

    public void upsert(long organizationId, long deviceId, LocalDate day, RuntimeAggregator.Daily d) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.runtime_stat_daily (device_id, day, organization_id, on_seconds, cycles, energy_wh_estimated, energy_source)
                        VALUES (:device, :day, :org, :on, :cycles, :energy, :source)
                        ON CONFLICT (device_id, day) DO UPDATE SET on_seconds = EXCLUDED.on_seconds, cycles = EXCLUDED.cycles,
                            energy_wh_estimated = EXCLUDED.energy_wh_estimated, energy_source = EXCLUDED.energy_source""")
                .param("device", deviceId).param("day", day).param("org", organizationId).param("on", (int) Math.min(Integer.MAX_VALUE, d.onSeconds()))
                .param("cycles", d.cycles()).param("energy", d.energyWh()).param("source", d.energySource())
                .update();
    }

    /** 효과 없음 이벤트 수를 하나 올린다 */
    public void incrementNoEffect(long organizationId, long deviceId, LocalDate day) {
        jdbc.sql("""
                        INSERT INTO data2flow_action.runtime_stat_daily (device_id, day, organization_id, no_effect_events)
                        VALUES (:device, :day, :org, 1)
                        ON CONFLICT (device_id, day) DO UPDATE SET no_effect_events = runtime_stat_daily.no_effect_events + 1""")
                .param("device", deviceId).param("day", day).param("org", organizationId).update();
    }

    public List<Map<String, Object>> findDaily(long organizationId, long deviceId, LocalDate from, LocalDate to) {
        return jdbc.sql("""
                        SELECT day, on_seconds, cycles, energy_wh_estimated, energy_source, no_effect_events FROM data2flow_action.runtime_stat_daily
                         WHERE organization_id = :org AND device_id = :device AND day >= :from AND day <= :to ORDER BY day""")
                .param("org", organizationId).param("device", deviceId).param("from", from).param("to", to)
                .query((rs, n) -> {
                    Map<String, Object> m = new java.util.LinkedHashMap<>();
                    m.put("date", rs.getObject("day", LocalDate.class).toString());
                    m.put("onSeconds", rs.getInt("on_seconds"));
                    m.put("cycles", rs.getInt("cycles"));
                    m.put("energyWh", rs.getObject("energy_wh_estimated"));
                    m.put("energySource", rs.getString("energy_source"));
                    m.put("noEffectEvents", rs.getInt("no_effect_events"));
                    return m;
                }).list();
    }

    public record StateRow(String capability, String attribute, Object value, Instant from, Instant to) {
    }
}
