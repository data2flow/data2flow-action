package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.Interlock;
import net.java21.data2flow.action.actuation.domain.InterlockEvaluator;
import net.java21.data2flow.action.actuation.domain.MetricValue;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.contracts.command.CommandPriority;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 인터락(ACT-06.02, BR-ACT-11). 규칙은 core에서 기기별로 읽어 캐시하고(API-ACT-44, {@code data2flow.config} INTERLOCK·DEVICE로 지움),
 * 조건 값은 상태 조건이면 action의 상태 쌍(보고 상태·보고 시각·연결 상태), 측정 조건이면 core 측정값(API-ACT-45)으로 본다.
 * 판정은 제어 창구 트랜잭션 밖에서 미리 한다(HTTP 호출을 트랜잭션 안에 두지 않는다).
 */
public class InterlockService {

    private final CoreClient core;
    private final ShadowRepository shadows;
    private final ControlProfileService profiles;
    private final Clock clock;
    private final Duration ttl;
    private final Map<Long, Entry> cache = new ConcurrentHashMap<>();

    private record Entry(List<Interlock> interlocks, Instant loadedAt) {
    }

    public InterlockService(CoreClient core, ShadowRepository shadows, ControlProfileService profiles, Clock clock, Duration ttl) {
        this.core = core;
        this.shadows = shadows;
        this.profiles = profiles;
        this.clock = clock;
        this.ttl = ttl;
    }

    /** 명령 하나를 판정한다. SAFETY(인터락 해소 뒤 안전 상태 복귀 등 시스템 명령)는 막지 않는다 */
    public InterlockEvaluator.Decision evaluate(ControlProfile device, CommandPriority priority, String capability, String command,
                                                Map<String, ?> args) {
        if (priority == CommandPriority.SAFETY) {
            return InterlockEvaluator.Decision.PASS;
        }
        List<Interlock> interlocks = interlocks(device.deviceId());
        if (interlocks.isEmpty()) {
            return InterlockEvaluator.Decision.PASS;
        }
        Instant now = clock.instant();
        return InterlockEvaluator.evaluate(interlocks, capability, command, args, c -> inputs(device, c, findInterlock(interlocks, c), now), now);
    }

    public List<Interlock> interlocks(long deviceId) {
        Instant now = clock.instant();
        Entry e = cache.get(deviceId);
        if (e != null && now.isBefore(e.loadedAt().plus(ttl))) {
            return e.interlocks();
        }
        List<Interlock> list = List.copyOf(core.interlocks(deviceId));
        cache.put(deviceId, new Entry(list, now));
        return list;
    }

    public void invalidate(long deviceId) {
        cache.remove(deviceId);
    }

    public void invalidateAll() {
        cache.clear();
    }

    private static Interlock findInterlock(List<Interlock> interlocks, Interlock.Condition c) {
        return interlocks.stream().filter(i -> i.condition() == c).findFirst().orElse(null);
    }

    private List<Optional<InterlockEvaluator.Input>> inputs(ControlProfile device, Interlock.Condition c, Interlock il, Instant now) {
        List<Long> devices = new ArrayList<>();
        if (c.deviceId() != null) {
            devices.add(c.deviceId());
        } else if (c.relation() != null && il != null && il.spaceId() != null) {
            devices.addAll(core.spaceDevices(il.spaceId(), c.relation(), c.capability(), !Boolean.FALSE.equals(il.includeChildren())));
        }
        List<Optional<InterlockEvaluator.Input>> out = new ArrayList<>();
        if (!c.state()) {
            if (devices.isEmpty()) {
                Long space = il != null && il.spaceId() != null ? il.spaceId() : device.spaceId();
                out.add(core.metricValue(c.metric(), null, space, now).map(InterlockService::metricInput));
            } else {
                for (Long d : devices) {
                    out.add(core.metricValue(c.metric(), d, null, now).map(InterlockService::metricInput));
                }
            }
            return out;
        }
        for (Long d : devices) {
            out.add(stateInput(d, device.organizationId(), c));
        }
        return out;
    }

    private static InterlockEvaluator.Input metricInput(MetricValue v) {
        return new InterlockEvaluator.Input(v.value(), v.measuredAt(), v.reportIntervalSec(), false, false);
    }

    private Optional<InterlockEvaluator.Input> stateInput(long deviceId, long organizationId, Interlock.Condition c) {
        Optional<ShadowRepository.ShadowRow> row = shadows.findByDeviceIdAndOrganizationId(deviceId, organizationId);
        if (row.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> state = row.get().shadow().reported().get(c.capability());
        Object value = state == null ? null : state.get(c.attribute());
        Integer interval = null;
        try {
            interval = profiles.find(deviceId).map(ControlProfile::reportIntervalSec).orElse(null);
        } catch (RuntimeException ignored) {
            // 보고 주기를 모르면 최소 5분으로 판정
        }
        boolean eventDriven = InterlockEvaluator.EVENT_CAPABILITIES.contains(c.capability());
        return Optional.of(new InterlockEvaluator.Input(value, row.get().shadow().reportedAt(), interval, eventDriven, row.get().offline()));
    }
}
