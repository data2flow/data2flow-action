package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.actuation.domain.PowerState;
import net.java21.data2flow.action.actuation.domain.ProtectionState;
import net.java21.data2flow.action.actuation.driver.DriverEventSink;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.DeviceStateRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository.ShadowRow;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.capability.CapabilityStates;
import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.capability.StateChange;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.command.SourceType;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import net.java21.data2flow.contracts.message.event.LoRaWanDownlinkAck;
import net.java21.data2flow.action.actuation.driver.lorawan.LoRaWanDriver;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 명령 추적(ACT-02.02)과 상태 쌍(ACT-02.04). 드라이버·가상 장비의 응답을 같은 경로로 처리한다(BR-ACT-25).
 *
 * <ul>
 *   <li>{@code device.command.ack}(EVT-ACT-06): SENT → ACKED / FAILED. 모르는 commandId는 무시하고 지표만 올린다(TC-ACT-037).</li>
 *   <li>{@code lorawan.downlink.ack}(EVT-ACT-09): 다운링크 큐 항목 ID(DB)로 명령을 찾아 위와 같은 경로로 처리한다(ACT-03.03).</li>
 *   <li>{@code device.state.reported}(EVT-ACT-07): 버전이 클 때만 반영(BR-ACT-05) → 목표와 같아진 진행 중 명령 APPLIED →
 *       수동 명령이면 수동 우선 시작(BR-ACT-08) → 보호 상태·상태 구간 갱신 → EVT-ACT-02 {@code device.state.changed}.</li>
 *   <li>{@code device.connectivity.changed}(EVT-DEV-02): 연결 상태 기록, 복귀하면 대기 명령 전송과 desired 재적용(BR-ACT-06·13).</li>
 *   <li>기한: SENT → TIMEOUT(TIMEOUT_ACK), ACKED → TIMEOUT(TIMEOUT_APPLY), QUEUED·QUEUED_FOR_DOWNLINK → FAILED(EXPIRED).</li>
 *   <li>상태 보고는 기기가 보낸 업링크이기도 하다: LoRaWAN Class A 다운링크 대기 명령을 바로 드라이버로 보낸다(ACT-07.02).</li>
 *   <li>APPLIED되면 기대 효과 확인을 예약한다(ACT-08.01).</li>
 * </ul>
 */
public class CommandTracker implements DriverEventSink {

    private static final Logger log = LoggerFactory.getLogger(CommandTracker.class);

    private final CommandRepository commands;
    private final ShadowRepository shadows;
    private final DeviceStateRepository deviceState;
    private final CommandEvents events;
    private final OutboxWriter outbox;
    private final ControlProfileService profiles;
    private final CommandDispatcher dispatcher;
    private final ControlFacade facade;
    private final CommandWaiter waiter;
    private final TransactionTemplate tx;
    private final ActionProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;
    private final ControlEffectService effects;

    public CommandTracker(CommandRepository commands, ShadowRepository shadows, DeviceStateRepository deviceState, CommandEvents events,
                          OutboxWriter outbox, ControlProfileService profiles, CommandDispatcher dispatcher, ControlFacade facade,
                          CommandWaiter waiter, PlatformTransactionManager txManager, ActionProperties properties, MeterRegistry meters,
                          Clock clock, ControlEffectService effects) {
        this.commands = commands;
        this.shadows = shadows;
        this.deviceState = deviceState;
        this.events = events;
        this.outbox = outbox;
        this.profiles = profiles;
        this.dispatcher = dispatcher;
        this.facade = facade;
        this.waiter = waiter;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
        this.effects = effects;
    }

    // ───────────── ack ─────────────

    @Override
    public void ack(long organizationId, DeviceCommandAck ack) {
        UUID id;
        try {
            id = UUID.fromString(ack.commandId());
        } catch (IllegalArgumentException | NullPointerException e) {
            unknownAck(ack);
            return;
        }
        Boolean known = tx.execute(status -> {
            Optional<Command> locked = commands.lock(id);
            if (locked.isEmpty() || locked.get().deviceId() != ack.deviceId() || locked.get().organizationId() != organizationId) {
                return false;
            }
            Command c = locked.get();
            if (c.status() != CommandStatus.REQUESTED && c.status() != CommandStatus.SENT) {
                return true;   // 이미 ACKED·APPLIED·끝 상태: 다시 보낸 ack
            }
            Instant now = clock.instant();
            Long spaceId = profiles.spaceOf(c.deviceId());
            if (ack.result() == DeviceCommandAck.Result.ACKED) {
                Command sent = c;
                if (c.status() == CommandStatus.REQUESTED) {
                    sent = events.transition(c, CommandEvents.to(c, CommandStatus.SENT, null, now, c.timeoutAt()), spaceId, null);
                }
                Duration apply = applyTimeout(c.deviceId());
                events.transition(sent, CommandEvents.to(sent, CommandStatus.ACKED, null, now, now.plus(apply)), spaceId, null);
            } else {
                String reason = ack.reason() == null ? CommandStatusReasons.DRIVER_ERROR : truncate(ack.reason());
                events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, reason, now, null), spaceId, null);
            }
            return true;
        });
        if (!Boolean.TRUE.equals(known)) {
            unknownAck(ack);
            return;
        }
        waiter.signal(id);
    }

    /**
     * EVT-ACT-09 {@code lorawan.downlink.ack}(ingress가 ChirpStack {@code event/ack}·{@code event/txack}에서 낸 것, ADR-054 남은 것 ①).
     * 드라이버가 다운링크를 등록할 때 남긴 큐 항목 ID({@code commands.downlink_queue_item_id})로 명령을 찾고, 표준 ack로 바꿔
     * {@link #ack}와 같은 경로로 SENT → ACKED/FAILED 한다. 메모리가 아니라 DB로 찾으므로 등록한 파드가 아니어도, 재시작한 뒤에도 이어진다.
     * 다시 받은 이벤트(이중 ingress·재전달)는 이미 바뀐 상태라 아무것도 하지 않는다(멱등). 모르는 큐 항목(다른 팀 기기 등)은 무시한다.
     */
    public void downlinkAck(long organizationId, LoRaWanDownlinkAck ack) {
        Optional<CommandRepository.DownlinkTarget> target = commands.findByDownlinkQueueItem(organizationId, ack.queueItemId());
        if (target.isEmpty()) {
            log.debug("모르는 다운링크 큐 항목의 결과를 무시합니다 queueItemId={} devEui={}", ack.queueItemId(), ack.devEui());
            meters.counter("data2flow_action_downlink_acks_total", "result", "unknown").increment();
            return;
        }
        CommandRepository.DownlinkTarget t = target.get();
        Optional<DeviceCommandAck> standard = LoRaWanDriver.commandAck(t.commandId(), t.deviceId(), ack, t.confirmed());
        if (standard.isEmpty()) {
            meters.counter("data2flow_action_downlink_acks_total", "result", "ignored").increment();
            return;   // 확인형 다운링크의 게이트웨이 송신(TXACK): 기기 확인을 기다린다
        }
        meters.counter("data2flow_action_downlink_acks_total", "result", standard.get().result().name()).increment();
        ack(organizationId, standard.get());
    }

    private void unknownAck(DeviceCommandAck ack) {
        log.warn("모르는 명령의 ack를 무시합니다 commandId={} device={}", ack.commandId(), ack.deviceId());
        meters.counter("data2flow_action_unknown_ack_total").increment();
    }

    // ───────────── 상태 보고 ─────────────

    @Override
    public void reported(long organizationId, DeviceStateReported report) {
        uplink(report.deviceId());
        if (uplinkOnly(report)) {
            // EVT-ACT-07 업링크 신호(pipeline: LoRaWAN ChirpStack event/up, ADR-049 남은 것 ②). 상태가 없으므로 상태 쌍·버전을 바꾸지 않고
            // EVT-ACT-02도 내지 않는다. 상태 쌍이 있는 기기(제어한 적 있는 기기)만 마지막 업링크 시각을 앞으로 당긴다(Class A 예상 시각 기준).
            tx.executeWithoutResult(status -> shadows.touchReportedAt(organizationId, report.deviceId(), report.reportedAt()));
            meters.counter("data2flow_action_uplink_signals_total").increment();
            return;
        }
        List<UUID> applied = tx.execute(status -> applyReport(organizationId, report));
        if (applied != null) {
            applied.forEach(waiter::signal);
        }
    }

    /** 기능 상태 없이 업링크가 있었다는 것만 알리는 보고(EVT-ACT-07 {@code capabilities:{}}) */
    static boolean uplinkOnly(DeviceStateReported report) {
        return report.capabilities() == null || report.capabilities().isEmpty();
    }

    private List<UUID> applyReport(long organizationId, DeviceStateReported report) {
        ShadowRow row = shadows.lockOrCreate(organizationId, report.deviceId());
        Instant now = clock.instant();
        Optional<DeviceShadow> next = row.shadow().withReported(report.version(), report.capabilities(), report.reportedAt());
        if (next.isEmpty()) {
            meters.counter("data2flow_action_stale_reports_total").increment();   // BR-ACT-05: 오래된 보고는 버린다
            return List.of();
        }
        DeviceShadow shadow = next.get();
        List<StateChange> changes = CapabilityStates.changes(row.shadow().reported(), shadow.reported());
        Optional<ControlProfile> profile = profileQuietly(report.deviceId());
        Long spaceId = profile.map(ControlProfile::spaceId).orElse(null);
        int overrideMinutes = profile.map(p -> p.settings().manualOverrideMinutes()).orElse(30);
        List<UUID> applied = new ArrayList<>();
        Command cause = null;
        for (Command c : commands.lockInFlight(report.deviceId())) {
            if (!shadow.isApplied(c.capability(), c.args())) {
                continue;
            }
            Command appliedCommand = events.transition(c, CommandEvents.to(c, CommandStatus.APPLIED, null, now, null), spaceId, null);
            effects.schedule(appliedCommand, spaceId, now);
            applied.add(c.id());
            cause = c;
            if (c.priority() == CommandPriority.MANUAL && overrideMinutes > 0) {
                deviceState.upsertManualOverride(organizationId, c.deviceId(), c.capability(), now.plus(Duration.ofMinutes(overrideMinutes)),
                        actor(c.source()), c.id(), now);
            }
        }
        updateProtection(organizationId, report.deviceId(), row.shadow(), shadow, now);
        for (StateChange ch : changes) {
            deviceState.recordState(organizationId, report.deviceId(), ch.capability(), ch.attribute(), ch.to(), now,
                    cause == null ? Map.of("type", "DEVICE_LOCAL") : Json.toMap(cause.source()), cause == null ? null : cause.id());
        }
        ShadowRow saved = row.withShadow(shadow);
        shadows.save(saved, now);
        DeviceStateChanged.Origin origin = applied.isEmpty() ? DeviceStateChanged.Origin.DEVICE_LOCAL : DeviceStateChanged.Origin.COMMAND;
        outbox.event(EventType.DEVICE_STATE_CHANGED, organizationId, new DeviceStateChanged(report.deviceId(), spaceId,
                        connectivity(saved.connectivity()), shadow.reported(), changes, shadow.reportedVersion(), shadow.delta(), now, origin),
                "state:" + report.deviceId() + ":" + report.version());
        return applied;
    }

    private void updateProtection(long organizationId, long deviceId, DeviceShadow before, DeviceShadow after, Instant now) {
        ProtectionState state = null;
        LocalDate today = LocalDate.ofInstant(now, ZoneOffset.UTC);
        for (String capability : after.reported().keySet()) {
            Optional<Boolean> was = PowerState.current(capability, before.reported().getOrDefault(capability, Map.of()));
            Optional<Boolean> is = PowerState.current(capability, after.reported().get(capability));
            if (is.isPresent() && !is.equals(was)) {
                if (state == null) {
                    state = deviceState.findProtection(organizationId, deviceId);
                }
                state = state.withPower(is.get(), now, today);
            }
        }
        if (state != null) {
            deviceState.saveProtection(organizationId, deviceId, state, now);
        }
    }

    // ───────────── Class A 업링크(ACT-07.02) ─────────────

    /**
     * 기기 업링크(상태 보고) 직후: Class A 다운링크 대기 명령을 다시 요청 상태로 두고 드라이버(ChirpStack 큐 등록)로 보낸다.
     * 유효 시각이 지난 명령은 FAILED(EXPIRED).
     */
    public void uplink(long deviceId) {
        List<UUID> dispatch = new ArrayList<>();
        tx.executeWithoutResult(status -> {
            Instant now = clock.instant();
            List<Command> waiting = commands.lockDownlinks(deviceId);
            if (waiting.isEmpty()) {
                return;
            }
            Long spaceId = profiles.spaceOf(deviceId);
            for (Command c : waiting) {
                if (c.validUntil() != null && now.isAfter(c.validUntil())) {
                    events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.EXPIRED, now, null), spaceId, null);
                    continue;
                }
                events.transition(c, CommandEvents.to(c, CommandStatus.REQUESTED, "UPLINK", now, now), spaceId, null);
                dispatch.add(c.id());
            }
        });
        dispatch.forEach(dispatcher::dispatch);
    }

    // ───────────── 연결 상태 ─────────────

    /** EVT-DEV-02. 복귀하면 대기 명령을 보내고, 모델이 재적용을 켜 두었으면 남은 차이를 다시 적용한다 */
    public void connectivity(long organizationId, DeviceConnectivityChanged change) {
        boolean online = change.to() == DeviceConnectivityChanged.Connectivity.ONLINE;
        List<UUID> toDispatch = new ArrayList<>();
        List<CommandRequest> reapply = new ArrayList<>();
        tx.executeWithoutResult(status -> {
            ShadowRow row = shadows.lockOrCreate(organizationId, change.deviceId());
            Instant now = clock.instant();
            shadows.save(row.withConnectivity(change.to().name()), now);
            if (!online) {
                return;
            }
            Long spaceId = profiles.spaceOf(change.deviceId());
            Set<String> busy = new LinkedHashSet<>();
            for (Command c : commands.lockQueued(change.deviceId())) {
                if (c.validUntil() != null && now.isAfter(c.validUntil())) {
                    events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.EXPIRED, now, null), spaceId, null);
                    continue;
                }
                events.transition(c, CommandEvents.to(c, CommandStatus.REQUESTED, "RECONNECT", now, now), spaceId, null);
                toDispatch.add(c.id());
                busy.add(c.capability());
            }
            commands.lockInFlight(change.deviceId()).forEach(c -> busy.add(c.capability()));
            Optional<ControlProfile> profile = profileQuietly(change.deviceId());
            if (profile.isEmpty()) {
                return;
            }
            Map<String, Map<String, Object>> delta = row.shadow().delta();
            for (Map.Entry<String, Map<String, Object>> e : delta.entrySet()) {
                ModelCapability mc = profile.get().capabilities().get(e.getKey());
                if (mc == null || !mc.reapplyOnReconnect() || busy.contains(e.getKey())) {
                    continue;
                }
                CommandSource source = row.desiredSource() == null || row.desiredSource().type() == SourceType.SCENE
                        ? CommandSource.system() : row.desiredSource();
                // BR-ACT-06: 재적용 명령의 우선순위는 원래 명령을 따른다(출처를 그대로 쓴다)
                reapply.add(new CommandRequest(organizationId, change.deviceId(), e.getKey(), "set", e.getValue(), source, null,
                        ActionIdempotencyKeys.of("reapply", Long.toString(change.deviceId()),
                                Long.toString(row.shadow().desiredVersion()), e.getKey()),
                        null, null, null, false, false));
            }
        });
        toDispatch.forEach(dispatcher::dispatch);
        for (CommandRequest r : reapply) {
            try {
                facade.submit(r);
            } catch (RuntimeException e) {
                log.warn("재연결 재적용 실패 device={} capability={}: {}", r.deviceId(), r.capability(), e.toString());
            }
        }
    }

    // ───────────── 기한 ─────────────

    /** 기한이 된 명령을 처리한다(기한 작업이 1초마다). 처리한 건수 */
    public int processDue() {
        List<UUID> dispatch = new ArrayList<>();
        List<UUID> recheck = new ArrayList<>();
        Integer handled = tx.execute(status -> {
            Instant now = clock.instant();
            int n = 0;
            for (CommandRepository.Due due : commands.lockDue(now, properties.scheduler().batch(), properties.env())) {
                n++;
                switch (due.status()) {
                    case REQUESTED -> dispatch.add(due.id());
                    case DELAYED -> recheck.add(due.id());
                    case SENT -> expire(due.id(), CommandStatus.TIMEOUT, CommandStatusReasons.TIMEOUT_ACK, now);
                    case ACKED -> expire(due.id(), CommandStatus.TIMEOUT, CommandStatusReasons.TIMEOUT_APPLY, now);
                    case QUEUED, QUEUED_FOR_DOWNLINK -> expire(due.id(), CommandStatus.FAILED, CommandStatusReasons.EXPIRED, now);
                    default -> {
                    }
                }
            }
            return n;
        });
        dispatch.forEach(dispatcher::dispatch);
        recheck.forEach(facade::recheckDelayed);
        return handled == null ? 0 : handled;
    }

    private void expire(UUID id, CommandStatus status, String reason, Instant now) {
        commands.lock(id).ifPresent(c -> {
            events.transition(c, CommandEvents.to(c, status, reason, now, null), profiles.spaceOf(c.deviceId()), null);
            waiter.signal(id);
        });
    }

    // ───────────── 도움 ─────────────

    private Duration applyTimeout(long deviceId) {
        return profileQuietly(deviceId).map(ControlProfile::driver)
                .map(d -> d.applyTimeout(properties.command().applyTimeout()))
                .orElse(properties.command().applyTimeout());
    }

    private Optional<ControlProfile> profileQuietly(long deviceId) {
        try {
            return profiles.find(deviceId);
        } catch (RuntimeException e) {
            log.debug("제어 프로필을 읽지 못했습니다 device={}: {}", deviceId, e.toString());
            return Optional.empty();
        }
    }

    private static long actor(CommandSource s) {
        if (s.userId() != null) {
            return s.userId();
        }
        return s.approvedBy() == null ? 0L : s.approvedBy();
    }

    private static DeviceStateChanged.Connectivity connectivity(String c) {
        try {
            return DeviceStateChanged.Connectivity.valueOf(c);
        } catch (IllegalArgumentException | NullPointerException e) {
            return DeviceStateChanged.Connectivity.UNKNOWN;
        }
    }

    private static String truncate(String reason) {
        return reason.length() <= CommandStatusReasons.MAX_LENGTH ? reason : reason.substring(0, CommandStatusReasons.MAX_LENGTH);
    }
}
