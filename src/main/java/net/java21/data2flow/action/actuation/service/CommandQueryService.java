package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.actuation.domain.PowerState;
import net.java21.data2flow.action.actuation.domain.ProtectionGuard;
import net.java21.data2flow.action.actuation.domain.ProtectionState;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverHealth;
import net.java21.data2flow.action.actuation.driver.DriverRegistry;
import net.java21.data2flow.action.actuation.dto.CommandDtos.CommandResponse;
import net.java21.data2flow.action.actuation.dto.CommandDtos.SourceView;
import net.java21.data2flow.action.actuation.dto.DeviceDtos;
import net.java21.data2flow.action.actuation.repository.CommandEventRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository.CommandQuery;
import net.java21.data2flow.action.actuation.repository.DeviceStateRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository.ShadowRow;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.capability.AttributeConstraint;
import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.capability.CapabilityDefinition;
import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.identity.CurrentUserHolder;
import net.java21.data2flow.contracts.web.CursorListApiResponse;
import net.java21.data2flow.contracts.web.CursorParams;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 명령·기기 상태 조회와 사용자 동작(API-ACT-02·03·04·06·31). core-api가 권한·공간 범위를 먼저 보고 넘기지만, 신원 헤더가 있으면 여기서도
 * 같은 권한을 다시 확인한다(기본 거부, IAM-04.05). 다른 조직의 명령·기기는 404다.
 */
public class CommandQueryService {

    private final CommandRepository commands;
    private final CommandEventRepository timeline;
    private final ShadowRepository shadows;
    private final DeviceStateRepository deviceState;
    private final CommandEvents events;
    private final ControlProfileService profiles;
    private final DriverRegistry drivers;
    private final RoleChecker roleChecker;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final CapabilityCatalog catalog = CapabilityCatalog.standard();

    public CommandQueryService(CommandRepository commands, CommandEventRepository timeline, ShadowRepository shadows,
                               DeviceStateRepository deviceState, CommandEvents events, ControlProfileService profiles,
                               DriverRegistry drivers, RoleChecker roleChecker, PlatformTransactionManager txManager, Clock clock) {
        this.commands = commands;
        this.timeline = timeline;
        this.shadows = shadows;
        this.deviceState = deviceState;
        this.events = events;
        this.profiles = profiles;
        this.drivers = drivers;
        this.roleChecker = roleChecker;
        this.tx = new TransactionTemplate(txManager);
        this.clock = clock;
    }

    /** API-ACT-02 명령 상세(타임라인 포함) */
    public CommandResponse get(long organizationId, UUID commandId) {
        Command c = commands.findByIdAndOrganizationId(commandId, organizationId)
                .orElseThrow(() -> new BusinessException(ActionErrorCode.COMMAND_NOT_FOUND));
        requireDevice(Permission.DEV_READ, organizationId, c.deviceId(), ActionErrorCode.COMMAND_NOT_FOUND);
        return CommandResponse.of(c, timeline.findByCommand(organizationId, c.id()), null);
    }

    /** API-ACT-02 기기별 명령 이력(커서 목록, 최신순). 거부·차단된 명령도 나온다(ACT-04.03) */
    public CursorListApiResponse<CommandResponse> list(CommandQuery query, String cursor, Integer size) {
        if (query.deviceId() != null) {
            requireDevice(Permission.DEV_READ, query.organizationId(), query.deviceId(), ActionErrorCode.DEVICE_NOT_FOUND);
        }
        CursorParams params = CursorParams.of(cursor, size);
        Instant cursorAt = null;
        UUID cursorId = null;
        if (params.cursor() != null) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(params.cursor()), StandardCharsets.UTF_8).split("\\|", 2);
                cursorAt = Instant.parse(parts[0]);
                cursorId = UUID.fromString(parts[1]);
            } catch (RuntimeException e) {
                throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                        List.of(new FieldErrorDetail("cursor", "INVALID", "올바르지 않은 커서입니다")));
            }
        }
        List<Command> rows = commands.listByDevice(query, cursorAt, cursorId, params.fetchSize());
        String next = null;
        if (rows.size() > params.size()) {
            rows = rows.subList(0, params.size());
            Command last = rows.get(rows.size() - 1);
            next = Base64.getUrlEncoder().withoutPadding().encodeToString((last.requestedAt() + "|" + last.id()).getBytes(StandardCharsets.UTF_8));
        }
        return CursorListApiResponse.of(params.size(), rows.stream().map(c -> CommandResponse.of(c, null, null)).toList(), next);
    }

    /** API-ACT-02 취소: QUEUED·DELAYED·QUEUED_FOR_DOWNLINK만 */
    public CommandResponse cancel(long organizationId, UUID commandId) {
        Command found = commands.findByIdAndOrganizationId(commandId, organizationId)
                .orElseThrow(() -> new BusinessException(ActionErrorCode.COMMAND_NOT_FOUND));
        Long spaceId = requireDevice(Permission.DEVICE_CONTROL, organizationId, found.deviceId(), ActionErrorCode.COMMAND_NOT_FOUND);
        Command cancelled = tx.execute(status -> {
            Command c = commands.lock(commandId).orElseThrow(() -> new BusinessException(ActionErrorCode.COMMAND_NOT_FOUND));
            if (!c.status().cancellable()) {
                throw new BusinessException(ActionErrorCode.COMMAND_NOT_CANCELLABLE);
            }
            return events.transition(c, CommandEvents.to(c, CommandStatus.CANCELLED, "USER_CANCELLED", clock.instant(), null), spaceId, null);
        });
        return CommandResponse.of(cancelled, timeline.findByCommand(organizationId, commandId), null);
    }

    /** API-ACT-04 상태 쌍 */
    public DeviceDtos.ShadowResponse shadow(long organizationId, long deviceId) {
        requireDevice(Permission.DEV_READ, organizationId, deviceId, ActionErrorCode.DEVICE_NOT_FOUND);
        return shadowView(shadows.findByDeviceIdAndOrganizationId(deviceId, organizationId).orElse(null));
    }

    /** API-ACT-03 기기 제어 정보(컨트롤 생성용) */
    public DeviceDtos.ControlResponse control(long organizationId, long deviceId) {
        ControlProfile p = profile(organizationId, deviceId, ActionErrorCode.DEVICE_NOT_FOUND);
        requireSpace(Permission.DEV_READ, p.spaceId(), ActionErrorCode.DEVICE_NOT_FOUND);
        Instant now = clock.instant();
        Optional<ShadowRow> row = shadows.findByDeviceIdAndOrganizationId(deviceId, organizationId);
        List<DeviceDtos.CapabilityView> caps = new ArrayList<>();
        Instant nextAllowed = null;
        ProtectionState protection = deviceState.findProtection(organizationId, deviceId);
        for (Map.Entry<String, ModelCapability> e : p.capabilities().entrySet()) {
            Optional<CapabilityDefinition> def = catalog.find(e.getKey());
            Map<String, Object> effective = new LinkedHashMap<>();
            Map<String, AttributeConstraint> limits = p.settings().limitsFor(e.getKey());
            for (String attr : unionKeys(e.getValue().constraints(), limits)) {
                AttributeConstraint model = e.getValue().constraints().getOrDefault(attr, AttributeConstraint.NONE);
                try {
                    effective.put(attr, model.intersect(limits.get(attr)));
                } catch (IllegalArgumentException ex) {
                    effective.put(attr, model);
                }
            }
            caps.add(new DeviceDtos.CapabilityView(e.getKey(), def.map(CapabilityDefinition::version).orElse(1),
                    def.map(CapabilityDefinition::attributes).orElse(null), def.map(CapabilityDefinition::commands).orElse(null), effective));
            if (e.getValue().protection() != null && row.isPresent()) {
                Map<String, Object> reported = row.get().shadow().reported().get(e.getKey());
                Optional<Boolean> current = reported == null ? Optional.empty() : PowerState.current(e.getKey(), reported);
                if (current.isPresent()) {
                    ProtectionGuard.Decision d = ProtectionGuard.evaluate(e.getValue().protection(), protection, current,
                            Optional.of(!current.get()), now, LocalDate.ofInstant(now, ZoneOffset.UTC));
                    if (d.kind() == ProtectionGuard.Kind.DELAY && (nextAllowed == null || d.executeAfter().isAfter(nextAllowed))) {
                        nextAllowed = d.executeAfter();
                    }
                }
            }
        }
        DeviceDtos.ManualOverrideView override = deviceState.findManualOverrides(organizationId, deviceId, now).stream().findFirst()
                .map(o -> new DeviceDtos.ManualOverrideView(o.capability(), o.until(), Long.toString(o.setBy()))).orElse(null);
        List<DeviceDtos.PendingView> pending = commands.findOpenByDevice(organizationId, deviceId).stream()
                .map(c -> new DeviceDtos.PendingView(c.id().toString(), c.capability(), c.command(), c.status().name())).toList();
        DeviceDtos.DriverView driver = p.driver() == null ? null
                : new DeviceDtos.DriverView(p.driver().driverId() == null ? null : p.driver().driverId().toString(), null, p.driver().type(),
                drivers.find(p.driver().type()).isPresent() ? "OK" : "UNAVAILABLE");
        return new DeviceDtos.ControlResponse(p.controllable() && drivers.find(p.driver().type()).isPresent(), driver, caps,
                shadowView(row.orElse(null)), override, nextAllowed == null ? null : new DeviceDtos.ProtectionView(nextAllowed), pending, null);
    }

    /** API-ACT-06 수동 우선 해제("자동으로 되돌리기") */
    public void releaseManualOverride(long organizationId, long deviceId, String capability) {
        requireDevice(Permission.DEVICE_CONTROL, organizationId, deviceId, ActionErrorCode.DEVICE_NOT_FOUND);
        deviceState.deleteManualOverride(organizationId, deviceId, capability == null || capability.isBlank() ? null : capability);
    }

    /** API-ACT-31 연결 확인. 정의(종류·설정)는 core가 넘긴다 */
    public DeviceDtos.HealthcheckResponse healthcheck(Long driverId, DeviceDtos.HealthcheckRequest request) {
        var driver = drivers.find(request == null ? null : request.type())
                .orElseThrow(() -> new BusinessException(ActionErrorCode.DRIVER_NOT_FOUND));
        DriverHealth h = driver.healthCheck(new DriverConfig(driverId, request.type(), request.config()));
        if (!h.ok()) {
            throw new BusinessException(ActionErrorCode.DRIVER_HEALTHCHECK_FAILED, h.message() == null ? h.errorKind() : h.message());
        }
        return new DeviceDtos.HealthcheckResponse(true, h.latencyMs(), h.capabilities(), null);
    }

    // ───────────── 도움 ─────────────

    private DeviceDtos.ShadowResponse shadowView(ShadowRow row) {
        if (row == null) {
            DeviceShadow s = DeviceShadow.EMPTY;
            return new DeviceDtos.ShadowResponse(s.desired(), 0, null, s.reported(), 0, null, Map.of(), "UNKNOWN");
        }
        DeviceShadow s = row.shadow();
        return new DeviceDtos.ShadowResponse(s.desired(), s.desiredVersion(), SourceView.of(row.desiredSource()), s.reported(),
                s.reportedVersion(), s.reportedAt(), s.delta(), row.connectivity());
    }

    private Long requireDevice(Permission permission, long organizationId, long deviceId, ActionErrorCode notFound) {
        if (CurrentUserHolder.find().isEmpty()) {
            return profiles.spaceOf(deviceId);
        }
        ControlProfile p = profile(organizationId, deviceId, notFound);
        requireSpace(permission, p.spaceId(), notFound);
        return p.spaceId();
    }

    private void requireSpace(Permission permission, Long spaceId, ActionErrorCode notFound) {
        if (CurrentUserHolder.find().isPresent()) {
            roleChecker.require(permission, spaceId, notFound);
        }
    }

    private ControlProfile profile(long organizationId, long deviceId, ActionErrorCode notFound) {
        return profiles.find(deviceId).filter(x -> x.organizationId() == organizationId)
                .orElseThrow(() -> new BusinessException(notFound));
    }

    private static List<String> unionKeys(Map<String, ?> a, Map<String, ?> b) {
        List<String> out = new ArrayList<>(a.keySet());
        b.keySet().stream().filter(k -> !out.contains(k)).forEach(out::add);
        return out;
    }
}
