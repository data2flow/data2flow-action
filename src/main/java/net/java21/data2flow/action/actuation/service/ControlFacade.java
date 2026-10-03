package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.ControlSettings;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.actuation.domain.PowerState;
import net.java21.data2flow.action.actuation.domain.ProtectionGuard;
import net.java21.data2flow.action.actuation.domain.RateGuards;
import net.java21.data2flow.action.actuation.domain.SandboxGuard;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.DeviceStateRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository.ShadowRow;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.contracts.audit.AuditActorType;
import net.java21.data2flow.contracts.audit.AuditCause;
import net.java21.data2flow.contracts.audit.AuditEvent;
import net.java21.data2flow.contracts.audit.AuditRecorder;
import net.java21.data2flow.contracts.audit.AuditResult;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.capability.ArgViolation;
import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.capability.CommandArgsValidator;
import net.java21.data2flow.contracts.capability.CommandValidation;
import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import net.java21.data2flow.contracts.command.SourceType;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 제어 창구(Control Facade, ACT-02.01, ADR-009). 화면(core 내부 API)·플로우(action.commands 큐)·재연결 재적용 등 모든 명령이 여기를
 * 지나고, 드라이버는 여기서만 부른다(드라이버를 직접 부르는 경로는 없다).
 *
 * <p>검사 순서(BR-ACT-01): 멱등(BR-ACT-02) → 권한 → 샌드박스 → 기능 스키마 → 모델 제약 → 조직 절대 한계 → [비상 정지: M4] → 수동 우선 →
 * [인터락: M4] → 변경 없음(BR-ACT-04) → 보호 → 진동·최소 간격 → 오프라인 판단 → 드라이버 호출. 앞 단계에서 걸리면 뒤 단계는 보지 않고,
 * 거부된 명령도 기록한다(ACT-04.03). 상태가 바뀔 때마다 타임라인과 EVT-ACT-01을 같은 트랜잭션에 남기고, 제어 명령 감사(IAM-06.01
 * {@code DEVICE_COMMAND})도 같은 트랜잭션의 아웃박스로 보낸다.
 */
public class ControlFacade {

    public static final String AUDIT_DEVICE_COMMAND = "DEVICE_COMMAND";
    private static final Logger log = LoggerFactory.getLogger(ControlFacade.class);

    private final CommandRepository commands;
    private final ShadowRepository shadows;
    private final DeviceStateRepository deviceState;
    private final CommandEvents events;
    private final ControlProfileService profiles;
    private final SandboxRegistry sandbox;
    private final RoleChecker roleChecker;
    private final AuditRecorder audit;
    private final CommandDispatcher dispatcher;
    private final TransactionTemplate tx;
    private final ActionProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;
    private final CapabilityCatalog catalog = CapabilityCatalog.standard();

    public ControlFacade(CommandRepository commands, ShadowRepository shadows, DeviceStateRepository deviceState, CommandEvents events,
                         ControlProfileService profiles, SandboxRegistry sandbox, RoleChecker roleChecker, AuditRecorder audit,
                         CommandDispatcher dispatcher, PlatformTransactionManager txManager, ActionProperties properties,
                         MeterRegistry meters, Clock clock) {
        this.commands = commands;
        this.shadows = shadows;
        this.deviceState = deviceState;
        this.events = events;
        this.profiles = profiles;
        this.sandbox = sandbox;
        this.roleChecker = roleChecker;
        this.audit = audit;
        this.dispatcher = dispatcher;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
    }

    /**
     * 명령 하나를 처리한다. 형식 오류(출처 누락)·없는 기기·권한 없음·제어 불가 기기는 기록 없이 {@link BusinessException},
     * 검증 이후의 거부·차단은 명령을 기록하고 {@link Outcome#rejection()}으로 알린다.
     */
    public Outcome submit(CommandRequest req) {
        Optional<Command> existing = commands.findByKey(req.organizationId(), req.idempotencyKey());
        if (existing.isPresent()) {
            return replay(existing.get());
        }
        CommandSource source = requireSource(req.source());
        ControlProfile profile = profiles.find(req.deviceId())
                .filter(p -> p.organizationId() == req.organizationId())
                .orElseThrow(() -> new BusinessException(ActionErrorCode.DEVICE_NOT_FOUND));
        if (req.checkPermission()) {
            roleChecker.require(Permission.DEVICE_CONTROL, profile.spaceId(), ActionErrorCode.DEVICE_NOT_FOUND);
        }
        if (!profile.controllable()) {
            throw new BusinessException(ActionErrorCode.DEVICE_NOT_CONTROLLABLE);
        }
        CommandPriority priority = CommandPriority.forSource(source.type());
        Instant now = clock.instant();
        Command requested = new Command(UUID.randomUUID(), req.organizationId(), req.idempotencyKey(), req.deviceId(), req.capability(),
                req.command(), req.args(), priority, source, CommandStatus.REQUESTED, null, validUntil(req, profile.settings(), now),
                null, 0, now, null, null, null, null, now);
        Decided decided = tx.execute(status -> {
            if (!commands.insert(requested, properties.env())) {
                return null;   // 동시에 같은 키가 들어옴: 먼저 들어온 결과를 돌려준다
            }
            Map<String, Object> detail = new LinkedHashMap<>();
            if (req.requestedPriority() != null && req.requestedPriority() != priority) {
                detail.put("requestedPriorityIgnored", req.requestedPriority().name());   // BR-ACT-24
            }
            events.created(requested, profile.spaceId(), detail);
            Decided d = decide(requested, req, profile, now);
            audit(d.command(), profile, req.requestedPriority());
            return d;
        });
        if (decided == null) {
            return replay(commands.findByKey(req.organizationId(), req.idempotencyKey()).orElseThrow());
        }
        meters.counter("data2flow_action_commands_total", "status", decided.command().status().name(),
                "source", source.type().name()).increment();
        if (decided.command().status() == CommandStatus.REQUESTED) {
            dispatcher.dispatch(decided.command().id());
            Command latest = commands.findByIdAndOrganizationId(decided.command().id(), req.organizationId()).orElse(decided.command());
            return new Outcome(latest, false, null, null);
        }
        return new Outcome(decided.command(), false, decided.rejection(), decided.message());
    }

    /**
     * 보호 지연(DELAYED) 명령의 실행 시각이 되었다: 수동 우선·보호·오프라인을 다시 보고 보낸다(BR-ACT-10 "허용 시각까지 미룬 뒤 다시 검사").
     */
    public void recheckDelayed(UUID commandId) {
        Boolean dispatch = tx.execute(status -> {
            Optional<Command> locked = commands.lock(commandId);
            if (locked.isEmpty() || locked.get().status() != CommandStatus.DELAYED) {
                return false;
            }
            Command c = locked.get();
            Instant now = clock.instant();
            Optional<ControlProfile> profile = profiles.find(c.deviceId());
            Long spaceId = profile.map(ControlProfile::spaceId).orElse(null);
            if (c.validUntil() != null && now.isAfter(c.validUntil())) {
                events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.EXPIRED, now, null), spaceId, null);
                return false;
            }
            if (profile.isEmpty() || !profile.get().controllable()) {
                events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.DRIVER_UNAVAILABLE, now, null), spaceId, null);
                return false;
            }
            ControlProfile p = profile.get();
            ShadowRow row = shadows.lockOrCreate(c.organizationId(), c.deviceId());
            if (manualOverrideBlocks(c, p.settings(), now)) {
                events.transition(c, CommandEvents.to(c, CommandStatus.SKIPPED, CommandStatusReasons.MANUAL_OVERRIDE, now, null), spaceId, null);
                return false;
            }
            ModelCapability mc = p.capability(c.capability()).orElse(ModelCapability.PLAIN);
            ProtectionGuard.Decision protection = protection(c, mc, row, now);
            if (protection.kind() == ProtectionGuard.Kind.BLOCK) {
                events.transition(c, CommandEvents.to(c, CommandStatus.BLOCKED, CommandStatusReasons.PROTECTION, now, null), spaceId, null);
                return false;
            }
            if (protection.kind() == ProtectionGuard.Kind.DELAY) {
                commands.save(CommandEvents.delayed(c, protection.executeAfter()));
                return false;
            }
            if (row.offline()) {
                events.transition(c, CommandEvents.to(c, CommandStatus.QUEUED, null, now, c.validUntil()), spaceId, null);
                return false;
            }
            events.transition(c, CommandEvents.to(c, CommandStatus.REQUESTED, "RECHECK", now, now), spaceId, null);
            return true;
        });
        if (Boolean.TRUE.equals(dispatch)) {
            dispatcher.dispatch(commandId);
        }
    }

    // ───────────── 검사 단계 ─────────────

    private Decided decide(Command c, CommandRequest req, ControlProfile p, Instant now) {
        Long spaceId = p.spaceId();
        if (req.expired()) {
            return end(c, CommandStatus.FAILED, CommandStatusReasons.EXPIRED, spaceId, null, null, now);
        }
        // 샌드박스(BR-ACT-23): 사용자 명령에는 적용하지 않으므로 필요할 때만 목록을 읽는다
        Long sourceSpace = req.sourceSpaceId();
        if (!p.virtual() && sourceSpace != null && SandboxGuard.forbidden(sandboxSpaces(c.source().type()), c.source().type(),
                sourceSpace, p.virtual())) {
            return end(c, CommandStatus.REJECTED, CommandStatusReasons.SANDBOX_FORBIDDEN, spaceId,
                    rejection(ActionErrorCode.ACT_SANDBOX_FORBIDDEN, List.of()), null, now);
        }
        Optional<ModelCapability> modelCapability = p.capability(c.capability());
        if (modelCapability.isEmpty()) {
            return end(c, CommandStatus.REJECTED, CommandStatusReasons.CAPABILITY_NOT_SUPPORTED, spaceId,
                    rejection(ActionErrorCode.CAPABILITY_NOT_SUPPORTED, List.of(new FieldErrorDetail("capability",
                            "CAPABILITY_NOT_SUPPORTED", "모델이 지원하지 않는 기능입니다: " + c.capability()))), null, now);
        }
        ModelCapability mc = modelCapability.get();
        CommandValidation validation = CommandArgsValidator.validate(catalog, c.capability(), c.command(), c.args(), mc.constraints(),
                p.settings().limitsFor(c.capability()));
        if (!validation.ok()) {
            ArgViolation v = validation.violations().get(0);
            return end(c, CommandStatus.REJECTED, reasonOf(v.reason()), spaceId, validationRejection(validation, v), null, now);
        }
        // [비상 정지 BR-ACT-12: ACT-06.03(M4)]
        ShadowRow row = shadows.lockOrCreate(c.organizationId(), c.deviceId());
        if (manualOverrideBlocks(c, p.settings(), now)) {
            return end(c, CommandStatus.SKIPPED, CommandStatusReasons.MANUAL_OVERRIDE, spaceId, null, null, now);
        }
        // [인터락 BR-ACT-11: ACT-06.02(M4)]
        if (row.shadow().noChange(c.capability(), c.args())) {
            return end(c, CommandStatus.SKIPPED, CommandStatusReasons.NO_CHANGE, spaceId, null, null, now);
        }
        ProtectionGuard.Decision protection = protection(c, mc, row, now);
        if (protection.kind() == ProtectionGuard.Kind.BLOCK) {
            return end(c, CommandStatus.BLOCKED, CommandStatusReasons.PROTECTION, spaceId,
                    rejection(ActionErrorCode.COMMAND_BLOCKED, List.of(), "PROTECTION"), "하루 최대 반복 횟수를 넘었습니다", now);
        }
        Optional<Boolean> target = PowerState.target(c.capability(), c.args());
        ControlSettings settings = p.settings();
        if (target.isPresent()) {
            Instant since = now.minusSeconds(settings.oscillation().windowSec());
            List<Boolean> recent = new ArrayList<>();
            for (Command prev : commands.findSentSince(c.deviceId(), c.capability(), since, c.id())) {
                PowerState.target(prev.capability(), prev.args()).ifPresent(recent::add);
            }
            if (RateGuards.oscillating(c.priority(), recent, target.get(), settings.oscillation().flips())) {
                log.warn("진동 차단(WARNING) device={} capability={} commands={}", c.deviceId(), c.capability(), recent.size() + 1);
                meters.counter("data2flow_action_oscillation_blocked_total").increment();
                return end(c, CommandStatus.BLOCKED, CommandStatusReasons.OSCILLATION, spaceId,
                        rejection(ActionErrorCode.COMMAND_BLOCKED, List.of(), "OSCILLATION"), "반대 명령이 반복되어 차단했습니다", now);
            }
        }
        Optional<Duration> wait = RateGuards.minInterval(c.priority(), commands.lastSentAt(c.deviceId(), c.capability(), c.id())
                .orElse(null), settings.minIntervalSec(), now);
        if (wait.isPresent()) {
            long seconds = Math.max(1, (wait.get().toMillis() + 999) / 1000);
            return end(c, CommandStatus.REJECTED, CommandStatusReasons.RATE_LIMITED, spaceId,
                    new Outcome.Rejection(ActionErrorCode.COMMAND_RATE_LIMITED, List.of(), new Object[]{seconds}, Duration.ofSeconds(seconds)),
                    null, now);
        }
        // 받아들임: 같은 기능의 대기 명령을 대체하고(BR-ACT-13) 원하는 상태를 바꾼다(목표 상태 설정, BR-ACT-03)
        for (Command old : commands.lockPending(c.deviceId(), c.capability(), c.id())) {
            events.transition(old, CommandEvents.to(old, CommandStatus.SUPERSEDED, null, now, null), spaceId, null);
        }
        DeviceShadow desired = row.shadow().withDesired(c.capability(), c.args());
        shadows.save(row.withDesired(desired, now, c.source()), now);
        if (protection.kind() == ProtectionGuard.Kind.DELAY) {
            Command delayed = CommandEvents.delayed(c, protection.executeAfter());
            return new Decided(events.transition(c, delayed, spaceId, null), null, null);
        }
        if (row.offline()) {
            return new Decided(events.transition(c, CommandEvents.to(c, CommandStatus.QUEUED, null, now, c.validUntil()), spaceId, null),
                    null, null);
        }
        return new Decided(c, null, null);   // REQUESTED, 커밋 뒤 드라이버 호출
    }

    private boolean manualOverrideBlocks(Command c, ControlSettings settings, Instant now) {
        if (!c.priority().automatic()) {
            return false;
        }
        Instant until = deviceState.findManualOverride(c.organizationId(), c.deviceId(), c.capability())
                .map(DeviceStateRepository.ManualOverride::until).orElse(null);
        return RateGuards.manualOverrideBlocks(c.priority(), until, now, settings.scheduleRespectsManualOverride());
    }

    private ProtectionGuard.Decision protection(Command c, ModelCapability mc, ShadowRow row, Instant now) {
        if (mc.protection() == null) {
            return ProtectionGuard.Decision.PASS;
        }
        Map<String, Object> reported = row.shadow().reported().get(c.capability());
        return ProtectionGuard.evaluate(mc.protection(), deviceState.findProtection(c.organizationId(), c.deviceId()),
                reported == null ? Optional.empty() : PowerState.current(c.capability(), reported),
                PowerState.target(c.capability(), c.args()), now, LocalDate.ofInstant(now, ZoneOffset.UTC));
    }

    private java.util.Set<Long> sandboxSpaces(SourceType type) {
        return switch (type) {
            case USER, BULK, SYSTEM -> java.util.Set.of();
            default -> sandbox.spaces();
        };
    }

    private Decided end(Command c, CommandStatus status, String reason, Long spaceId, Outcome.Rejection rejection, String message,
                        Instant now) {
        Command after = events.transition(c, CommandEvents.to(c, status, reason, now, null), spaceId, message);
        return new Decided(after, rejection, message);
    }

    private static String reasonOf(ArgViolation.Reason r) {
        return switch (r) {
            case CAPABILITY_NOT_SUPPORTED, COMMAND_NOT_SUPPORTED -> CommandStatusReasons.CAPABILITY_NOT_SUPPORTED;
            case MODEL_CONSTRAINT -> CommandStatusReasons.MODEL_CONSTRAINT;
            case ABSOLUTE_LIMIT -> CommandStatusReasons.ABSOLUTE_LIMIT;
            default -> CommandStatusReasons.ARGS_INVALID;
        };
    }

    private static Outcome.Rejection validationRejection(CommandValidation validation, ArgViolation first) {
        ActionErrorCode code = ActionErrorCode.valueOf(validation.resultCode().orElse(ActionErrorCode.COMMAND_ARGS_INVALID.name()));
        Object[] args = first.min() != null || first.max() != null
                ? new Object[]{fmt(first.min()), fmt(first.max())} : new Object[0];
        return new Outcome.Rejection(code, validation.fieldErrors(), args, null);
    }

    private static String fmt(Double v) {
        return v == null ? "" : java.math.BigDecimal.valueOf(v).stripTrailingZeros().toPlainString();
    }

    private static Outcome.Rejection rejection(ActionErrorCode code, List<FieldErrorDetail> errors, Object... args) {
        return new Outcome.Rejection(code, errors, args, null);
    }

    private Instant validUntil(CommandRequest req, ControlSettings settings, Instant now) {
        if (req.validUntil() != null) {
            return req.validUntil();
        }
        int seconds = req.validitySeconds() == null ? settings.defaultValiditySec() : Math.clamp(req.validitySeconds(), 60, 3600);
        return now.plusSeconds(seconds);
    }

    private static CommandSource requireSource(CommandSource source) {
        if (source == null || source.type() == SourceType.SCENE || source.type() == SourceType.UNKNOWN) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("source.type", "INVALID", "지원하지 않는 출처입니다")));
        }
        try {
            return source.requireComplete();   // BR-ACT-15: AI는 승인자가 있어야 한다
        } catch (IllegalArgumentException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("source", "INCOMPLETE", e.getMessage())));
        }
    }

    /** 이전 결과를 그대로 돌려준다(BR-ACT-02). 거부된 명령이면 같은 오류로 */
    private Outcome replay(Command c) {
        return new Outcome(c, true, replayRejection(c), null);
    }

    static Outcome.Rejection replayRejection(Command c) {
        if (c.status() != CommandStatus.REJECTED && c.status() != CommandStatus.BLOCKED) {
            return null;
        }
        String reason = c.statusReason() == null ? "" : c.statusReason();
        ActionErrorCode code = switch (reason) {
            case CommandStatusReasons.SANDBOX_FORBIDDEN -> ActionErrorCode.ACT_SANDBOX_FORBIDDEN;
            case CommandStatusReasons.CAPABILITY_NOT_SUPPORTED -> ActionErrorCode.CAPABILITY_NOT_SUPPORTED;
            case CommandStatusReasons.MODEL_CONSTRAINT -> ActionErrorCode.COMMAND_ARG_OUT_OF_RANGE;
            case CommandStatusReasons.ABSOLUTE_LIMIT -> ActionErrorCode.COMMAND_ABSOLUTE_LIMIT;
            case CommandStatusReasons.RATE_LIMITED -> ActionErrorCode.COMMAND_RATE_LIMITED;
            case CommandStatusReasons.ARGS_INVALID -> ActionErrorCode.COMMAND_ARGS_INVALID;
            default -> ActionErrorCode.COMMAND_BLOCKED;
        };
        Object[] args = switch (code) {
            case COMMAND_ARG_OUT_OF_RANGE, COMMAND_ABSOLUTE_LIMIT -> new Object[]{"", ""};
            case COMMAND_RATE_LIMITED -> new Object[]{1};
            case COMMAND_BLOCKED -> new Object[]{reason};
            default -> new Object[0];
        };
        return new Outcome.Rejection(code, List.of(), args, code == ActionErrorCode.COMMAND_RATE_LIMITED ? Duration.ofSeconds(1) : null);
    }

    /** 제어 명령 감사(IAM-06.01 DEVICE_COMMAND, 자동 제어 원인 IAM-06.04) */
    private void audit(Command c, ControlProfile p, CommandPriority requestedPriority) {
        CommandSource s = c.source();
        AuditEvent.Builder b = AuditEvent.builder(c.organizationId(), AUDIT_DEVICE_COMMAND)
                .occurredAt(clock.instant())
                .target("DEVICE", Long.toString(c.deviceId()))
                .result(switch (c.status()) {
                    case REJECTED, FAILED -> AuditResult.FAILURE;
                    case BLOCKED -> AuditResult.DENIED;
                    default -> AuditResult.SUCCESS;
                })
                .detail("commandId", c.id().toString())
                .detail("deviceName", p.name())
                .detail("capability", c.capability())
                .detail("command", c.command())
                .detail("args", c.args())
                .detail("status", c.status().name())
                .detail("priority", c.priority().name())
                .detail("source", s);
        if (c.statusReason() != null) {
            b.detail("reason", c.statusReason());
        }
        if (requestedPriority != null && requestedPriority != c.priority()) {
            b.detail("requestedPriorityIgnored", requestedPriority.name());
        }
        switch (s.type()) {
            case USER, BULK -> b.actor(AuditActorType.USER, String.valueOf(s.userId()), null);
            case AI -> b.actor(AuditActorType.USER, String.valueOf(s.approvedBy()), null).detail("suggestionId", s.suggestionId());
            case FLOW -> b.actor(AuditActorType.FLOW, s.flowId(), null)
                    .cause(new AuditCause(s.flowId(), s.flowVersion(), s.nodeId(), s.triggerMessageId()));
            default -> b.actor(AuditActorType.SYSTEM, s.type().name(), null);
        }
        audit.record(b.build());
    }

    private record Decided(Command command, Outcome.Rejection rejection, String message) {
    }
}
