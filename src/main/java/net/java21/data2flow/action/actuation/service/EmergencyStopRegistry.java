package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.EmergencyStop;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 진행 중인 비상 정지(ACT-06.03, BR-ACT-12). 원천은 core({@code emergency_stops}, API-ACT-46)이고 파드마다 메모리에 둔다.
 * {@code data2flow.config} EMERGENCY_STOP(파드마다 받음)과 EVT-ACT-03을 받으면 1초 안에 다시 읽고, 놓쳐도 {@code ttl}(30초) 뒤 다시 읽는다.
 * 읽지 못했고 이전 목록도 없으면 자동 명령을 판정할 수 없으므로 예외를 던진다(호출 쪽이 재시도: 안전 쪽).
 */
public class EmergencyStopRegistry {

    private static final Logger log = LoggerFactory.getLogger(EmergencyStopRegistry.class);

    private final CoreClient core;
    private final Clock clock;
    private final Duration ttl;
    private volatile List<EmergencyStop> active;
    private volatile Instant loadedAt;

    public EmergencyStopRegistry(CoreClient core, Clock clock, Duration ttl) {
        this.core = core;
        this.clock = clock;
        this.ttl = ttl;
    }

    /** 이 우선순위·공간의 명령을 막는 비상 정지. MANUAL·SAFETY는 언제나 빈 값 */
    public Optional<EmergencyStop> blocking(long organizationId, CommandPriority priority, List<Long> spacePathIds) {
        if (!EmergencyStopChanged.Scope.blocks(priority)) {
            return Optional.empty();
        }
        return covering(organizationId, spacePathIds);
    }

    /** 이 공간을 덮는 비상 정지(API-ACT-03 {@code emergencyStop}) */
    public Optional<EmergencyStop> covering(long organizationId, List<Long> spacePathIds) {
        for (EmergencyStop s : current()) {
            if (s.organizationId() == organizationId && s.scope() != null && s.scope().covers(spacePathIds)) {
                return Optional.of(s);
            }
        }
        return Optional.empty();
    }

    public List<EmergencyStop> current() {
        Instant now = clock.instant();
        List<EmergencyStop> snapshot = active;
        if (snapshot != null && loadedAt != null && now.isBefore(loadedAt.plus(ttl))) {
            return snapshot;
        }
        try {
            return reload();
        } catch (RuntimeException e) {
            if (snapshot != null) {
                log.warn("비상 정지 목록을 다시 읽지 못해 이전 목록을 씁니다: {}", e.toString());
                return snapshot;
            }
            throw new IllegalStateException("비상 정지 목록을 읽을 수 없습니다(자동 명령은 다시 시도)", e);
        }
    }

    /** core에서 다시 읽는다 */
    public synchronized List<EmergencyStop> reload() {
        List<EmergencyStop> list = List.copyOf(core.activeEmergencyStops());
        active = list;
        loadedAt = clock.instant();
        return list;
    }

    /** EVT-ACT-03을 받은 파드는 core를 기다리지 않고 바로 반영한다 */
    public synchronized void apply(long organizationId, EmergencyStopChanged change, boolean started) {
        List<EmergencyStop> next = new ArrayList<>(active == null ? List.of() : active);
        next.removeIf(s -> s.emergencyStopId() == change.id());
        if (started) {
            next.add(new EmergencyStop(change.id(), organizationId, change.scope(), change.reason(), change.at()));
        }
        active = List.copyOf(next);
        if (loadedAt == null) {
            loadedAt = clock.instant();
        }
    }

    /** 다음 판정 때 다시 읽게 한다 */
    public void invalidate() {
        loadedAt = null;
    }
}
