package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.repository.CommandEventRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.CommandStatusChanged;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;

/**
 * 명령 상태 전이 기록(ACT-02.02): 행 저장 + 타임라인({@code command_events}) + EVT-ACT-01 {@code command.status.{status}}(아웃박스).
 * 호출 쪽 트랜잭션 안에서 부른다.
 */
@Component
public class CommandEvents {

    private final CommandRepository commands;
    private final CommandEventRepository timeline;
    private final OutboxWriter outbox;
    private final Clock clock;

    public CommandEvents(CommandRepository commands, CommandEventRepository timeline, OutboxWriter outbox, Clock clock) {
        this.commands = commands;
        this.timeline = timeline;
        this.outbox = outbox;
        this.clock = clock;
    }

    /** 새 명령(REQUESTED) 첫 기록 */
    public void created(Command c, Long spaceId, Map<String, Object> detail) {
        long eventId = timeline.insert(c.id(), c.organizationId(), c.requestedAt(), null, c.status().name(), c.statusReason(), detail);
        publish(c, spaceId, eventId, null);
    }

    /** {@code before} → {@code after} 전이를 저장하고 알린다. 상태가 같으면 행만 저장한다 */
    public Command transition(Command before, Command after, Long spaceId, String message) {
        commands.save(after);
        if (before.status() != after.status()) {
            long eventId = timeline.insert(after.id(), after.organizationId(), clock.instant(), before.status().name(),
                    after.status().name(), after.statusReason(), message == null ? null : Map.of("message", message));
            publish(after, spaceId, eventId, message);
        }
        return after;
    }

    /** 전이 + 타임라인 detail(인터락 ID 등) */
    public Command transition(Command before, Command after, Long spaceId, String message, Map<String, Object> detail) {
        commands.save(after);
        if (before.status() != after.status()) {
            Map<String, Object> d = new java.util.LinkedHashMap<>(detail == null ? Map.of() : detail);
            if (message != null) {
                d.put("message", message);
            }
            long eventId = timeline.insert(after.id(), after.organizationId(), clock.instant(), before.status().name(),
                    after.status().name(), after.statusReason(), d.isEmpty() ? null : d);
            publish(after, spaceId, eventId, message);
        }
        return after;
    }

    /** EVT-ACT-08 진동 차단(BR-ACT-07 WARNING 알람의 원천, ADR-048). 차단 전이와 같은 트랜잭션 */
    public void oscillationBlocked(Command c, Long spaceId, int flips, int windowSec) {
        outbox.event(EventType.CONTROL_OSCILLATION_BLOCKED, c.organizationId(),
                new net.java21.data2flow.contracts.message.event.OscillationBlocked(c.id(), c.deviceId(), spaceId, c.capability(), c.command(),
                        flips, windowSec, c.source(), clock.instant()), "oscillation:" + c.id());
    }

    /** 멱등 재요청: 처음 결과를 다시 알린다(BR-ACT-02, ACT-api §5.1) */
    public void republish(Command c, Long spaceId, String replayKey) {
        outbox.event(EventType.commandStatus(c.status()), c.organizationId(), payload(c, spaceId, null),
                "replay:" + replayKey + ":" + c.id());
    }

    private void publish(Command c, Long spaceId, long eventId, String message) {
        outbox.event(EventType.commandStatus(c.status()), c.organizationId(), payload(c, spaceId, message), "command-event:" + eventId);
    }

    private CommandStatusChanged payload(Command c, Long spaceId, String message) {
        return new CommandStatusChanged(c.id(), c.idempotencyKey(), c.deviceId(), spaceId, c.capability(), c.command(), c.args(),
                c.status(), c.statusReason(), message, c.source(), c.priority(), clock.instant());
    }

    /** 상태 전이 사본(시각 포함). 끝 상태면 finished_at을 채우고 기한을 지운다 */
    public static Command to(Command c, CommandStatus status, String reason, Instant now, Instant timeoutAt) {
        Instant sent = c.sentAt();
        Instant acked = c.ackedAt();
        Instant applied = c.appliedAt();
        switch (status) {
            case SENT -> sent = sent == null ? now : sent;
            case ACKED -> {
                sent = sent == null ? now : sent;
                acked = acked == null ? now : acked;
            }
            case APPLIED -> {
                sent = sent == null ? now : sent;
                acked = acked == null ? now : acked;
                applied = now;
            }
            default -> {
            }
        }
        boolean end = status.terminal();
        return new Command(c.id(), c.organizationId(), c.idempotencyKey(), c.deviceId(), c.capability(), c.command(), c.args(),
                c.priority(), c.source(), status, reason, c.validUntil(), c.executeAfter(),
                c.attempts(), c.requestedAt(), sent, acked, applied, end ? now : c.finishedAt(), end ? null : timeoutAt);
    }

    /** 실행 예정 시각을 바꾼 사본(DELAYED) */
    public static Command delayed(Command c, Instant executeAfter) {
        return new Command(c.id(), c.organizationId(), c.idempotencyKey(), c.deviceId(), c.capability(), c.command(), c.args(),
                c.priority(), c.source(), CommandStatus.DELAYED, "PROTECTION", c.validUntil(), executeAfter, c.attempts(),
                c.requestedAt(), c.sentAt(), c.ackedAt(), c.appliedAt(), c.finishedAt(), null);
    }

    /** 드라이버 호출 횟수를 하나 올리고 기한을 바꾼 사본 */
    public static Command attempted(Command c, Instant timeoutAt) {
        return new Command(c.id(), c.organizationId(), c.idempotencyKey(), c.deviceId(), c.capability(), c.command(), c.args(),
                c.priority(), c.source(), c.status(), c.statusReason(), c.validUntil(), c.executeAfter(), c.attempts() + 1,
                c.requestedAt(), c.sentAt(), c.ackedAt(), c.appliedAt(), c.finishedAt(), timeoutAt);
    }
}
