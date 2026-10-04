package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.PowerState;
import net.java21.data2flow.action.actuation.domain.RuntimeAggregator;
import net.java21.data2flow.action.actuation.repository.EffectCheckRepository;
import net.java21.data2flow.action.actuation.repository.RuntimeStatRepository;
import net.java21.data2flow.action.common.ActionProperties;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 장비별 가동 집계(ACT-08.02, BR-ACT-21): 상태 구간({@code device_state_history})의 전원 속성으로 하루 가동 시간·켜짐 횟수를 계산하고, 정격
 * 전력(API-ACT-40 {@code ratedPowerW}) × 가동 시간으로 에너지를 추정해 {@code runtime_stat_daily}에 둔다. 날짜 경계는 사이트 시간대.
 * 조회(API-ACT-35)는 일별 집계와 효과 없음 기록을 함께 준다.
 */
public class RuntimeStatsService {

    private final RuntimeStatRepository runtime;
    private final EffectCheckRepository effects;
    private final ControlProfileService profiles;
    private final ActionProperties properties;
    private final Clock clock;

    public RuntimeStatsService(RuntimeStatRepository runtime, EffectCheckRepository effects, ControlProfileService profiles,
                               ActionProperties properties, Clock clock) {
        this.runtime = runtime;
        this.effects = effects;
        this.profiles = profiles;
        this.properties = properties;
        this.clock = clock;
    }

    /** 어제·오늘 집계를 다시 계산한다(주기 작업). 갱신한 기기·날 수 */
    public int aggregateRecent() {
        ZoneId zone = properties.effect().zone();
        LocalDate today = LocalDate.ofInstant(clock.instant(), zone);
        int n = 0;
        for (LocalDate day : List.of(today.minusDays(1), today)) {
            Instant from = day.atStartOfDay(zone).toInstant();
            Instant to = day.plusDays(1).atStartOfDay(zone).toInstant();
            for (long[] d : runtime.findDevicesWithHistory(from, to)) {
                aggregate(d[0], d[1], day);
                n++;
            }
        }
        return n;
    }

    /** 기기 하루 집계 */
    public RuntimeAggregator.Daily aggregate(long organizationId, long deviceId, LocalDate day) {
        ZoneId zone = properties.effect().zone();
        Instant from = day.atStartOfDay(zone).toInstant();
        Instant dayEnd = day.plusDays(1).atStartOfDay(zone).toInstant();
        Instant now = clock.instant();
        Instant to = now.isBefore(dayEnd) ? now : dayEnd;
        List<RuntimeStatRepository.StateRow> rows = runtime.findPowerHistory(organizationId, deviceId, from, to);
        boolean initialOn = false;
        List<RuntimeAggregator.PowerChange> changes = new ArrayList<>();
        String capability = rows.stream().anyMatch(r -> "Switch".equals(r.capability())) ? "Switch"
                : rows.stream().findFirst().map(RuntimeStatRepository.StateRow::capability).orElse(null);
        for (RuntimeStatRepository.StateRow r : rows) {
            if (!r.capability().equals(capability)) {
                continue;
            }
            Optional<Boolean> on = PowerState.current(capability, Map.of(r.attribute(), r.value()));
            if (on.isEmpty()) {
                continue;
            }
            if (r.from().isBefore(from)) {
                initialOn = on.get();
            } else {
                changes.add(new RuntimeAggregator.PowerChange(r.from(), on.get()));
            }
        }
        Double rated = null;
        try {
            rated = profiles.find(deviceId).map(ControlProfile::ratedPowerW).orElse(null);
        } catch (RuntimeException ignored) {
            // 정격 전력을 모르면 에너지 없이 가동 시간만
        }
        RuntimeAggregator.Daily daily = RuntimeAggregator.aggregate(initialOn, changes, from, to, rated, null);
        runtime.upsert(organizationId, deviceId, day, daily);
        return daily;
    }

    /** API-ACT-35 {@code GET /devices/{device-id}/runtime?from=&to=} */
    public Map<String, Object> runtime(long organizationId, long deviceId, LocalDate from, LocalDate to) {
        ZoneId zone = properties.effect().zone();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", runtime.findDaily(organizationId, deviceId, from, to));
        out.put("noEffectEvents", effects.findNoEffect(organizationId, deviceId, from.atStartOfDay(zone).toInstant(),
                to.plusDays(1).atStartOfDay(zone).toInstant()).stream().map(c -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("at", c.finishedAt());
                    m.put("commandId", c.commandId().toString());
                    m.put("metric", c.metric());
                    m.put("expected", Map.of("direction", c.direction(), "withinMinutes", c.withinMinutes()));
                    Map<String, Object> observed = new LinkedHashMap<>();
                    observed.put("start", c.startValue());
                    observed.put("end", c.endValue());
                    m.put("observed", observed);
                    return m;
                }).toList());
        return out;
    }
}
