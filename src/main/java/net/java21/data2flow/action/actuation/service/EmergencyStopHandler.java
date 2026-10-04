package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 비상 정지 시작·해제 수신(EVT-ACT-03, ACT-06.03, BR-ACT-12). 시작이면 범위 안에서 대기 중이던 AUTO·SCHEDULE·AI 명령(DELAYED·QUEUED·
 * QUEUED_FOR_DOWNLINK)을 CANCELLED(EMERGENCY_STOP)로 끝낸다. 이 이벤트는 공유 큐로 한 파드만 받으므로 DB 작업만 하고, 파드별 목록은
 * {@code data2flow.config} EMERGENCY_STOP으로 다시 읽는다({@link EmergencyStopRegistry}).
 */
public class EmergencyStopHandler {

    private static final Logger log = LoggerFactory.getLogger(EmergencyStopHandler.class);

    private final EmergencyStopRegistry registry;
    private final CommandRepository commands;
    private final CommandEvents events;
    private final ControlProfileService profiles;
    private final TransactionTemplate tx;
    private final Clock clock;

    public EmergencyStopHandler(EmergencyStopRegistry registry, CommandRepository commands, CommandEvents events,
                                ControlProfileService profiles, PlatformTransactionManager txManager, Clock clock) {
        this.registry = registry;
        this.commands = commands;
        this.events = events;
        this.profiles = profiles;
        this.tx = new TransactionTemplate(txManager);
        this.clock = clock;
    }

    /** 시작: 대기 중 자동 명령 취소. 취소한 건수 */
    public int started(long organizationId, EmergencyStopChanged change) {
        registry.apply(organizationId, change, true);
        Integer n = tx.execute(s -> {
            Instant now = clock.instant();
            int cancelled = 0;
            for (Command c : commands.lockPendingAutomatic(organizationId)) {
                Optional<ControlProfile> p = profileQuietly(c.deviceId());
                List<Long> path = p.map(ControlProfile::spacePathIds).orElse(List.of());
                if (!change.scope().covers(path)) {
                    continue;
                }
                events.transition(c, CommandEvents.to(c, CommandStatus.CANCELLED, CommandStatusReasons.EMERGENCY_STOP, now, null),
                        p.map(ControlProfile::spaceId).orElse(null), "비상 정지" + (change.reason() == null ? "" : ": " + change.reason()));
                cancelled++;
            }
            return cancelled;
        });
        log.warn("비상 정지 시작 id={} 범위={} 대기 자동 명령 취소 {}건", change.id(), change.scope(), n);
        return n == null ? 0 : n;
    }

    /** 해제: 목록에서 뺀다(취소된 명령은 되살리지 않는다) */
    public void released(long organizationId, EmergencyStopChanged change) {
        registry.apply(organizationId, change, false);
        log.info("비상 정지 해제 id={}", change.id());
    }

    private Optional<ControlProfile> profileQuietly(long deviceId) {
        try {
            return profiles.find(deviceId);
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }
}
