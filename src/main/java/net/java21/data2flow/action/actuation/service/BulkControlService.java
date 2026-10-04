package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.BulkPolicy;
import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.SceneBulkRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * 일괄 제어(ACT-02.06, API-ACT-05, BR-ACT-17). 대상(기기 목록 또는 공간 + 기능)을 펼쳐 500대 이하·모든 기기가 기능 지원인지 확인하고,
 * 미리보기면 기기별 현재 → 목표·변경 여부를, 실행이면 {@code bulk_jobs}에 작업을 남기고 기기마다 제어 창구로 명령을 낸다(출처 BULK, 우선순위
 * MANUAL). 명령은 작업 ID + 기기 ID 멱등 키라 파드가 실행 중 죽어도 이어서 다시 내면 기기마다 한 번만 실행된다.
 */
public class BulkControlService {

    private static final Logger log = LoggerFactory.getLogger(BulkControlService.class);

    private final CoreClient core;
    private final ControlProfileService profiles;
    private final ShadowRepository shadows;
    private final SceneBulkRepository repository;
    private final CommandRepository commands;
    private final ControlFacade facade;
    private final RoleChecker roleChecker;
    private final Executor executor;
    private final Clock clock;

    public BulkControlService(CoreClient core, ControlProfileService profiles, ShadowRepository shadows, SceneBulkRepository repository,
                              CommandRepository commands, ControlFacade facade, RoleChecker roleChecker, Executor executor, Clock clock) {
        this.core = core;
        this.profiles = profiles;
        this.shadows = shadows;
        this.repository = repository;
        this.commands = commands;
        this.facade = facade;
        this.roleChecker = roleChecker;
        this.executor = executor;
        this.clock = clock;
    }

    /**
     * 일괄 요청.
     *
     * @param deviceIds       대상 기기(또는)
     * @param spaceId         대상 공간
     * @param includeChildren 하위 공간 포함
     * @param capability      기능
     * @param command         명령
     * @param args            인자
     */
    public record BulkRequest(List<Long> deviceIds, Long spaceId, boolean includeChildren, String capability, String command,
                              Map<String, Object> args) {
    }

    /** 미리보기(API-ACT-05 preview): 기기별 현재 → 목표, 바뀌는지, 경고(오프라인·수동 우선) */
    public Map<String, Object> preview(long organizationId, BulkRequest req) {
        roleChecker.require(Permission.DEVICE_CONTROL);
        List<ControlProfile> devices = resolve(organizationId, req);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ControlProfile p : devices) {
            var row = shadows.findByDeviceIdAndOrganizationId(p.deviceId(), organizationId);
            Map<String, Object> current = row.map(r -> r.shadow().reported().getOrDefault(req.capability(), Map.of())).orElse(Map.of());
            boolean willChange = row.map(r -> !r.shadow().isApplied(req.capability(), req.args())).orElse(true);
            List<String> warnings = new ArrayList<>();
            if (row.map(ShadowRepository.ShadowRow::offline).orElse(false)) {
                warnings.add("OFFLINE");
            }
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("deviceId", Long.toString(p.deviceId()));
            m.put("name", p.name());
            m.put("current", current);
            m.put("target", req.args());
            m.put("willChange", willChange);
            m.put("warnings", warnings);
            rows.add(m);
        }
        return Map.of("devices", rows);
    }

    /** 실행(202 {bulkJobId, total}). 명령 제출은 뒤에서 이어진다 */
    public Map<String, Object> start(long organizationId, long userId, BulkRequest req) {
        roleChecker.require(Permission.DEVICE_CONTROL);
        List<ControlProfile> devices = resolve(organizationId, req);
        Map<String, Object> target = new LinkedHashMap<>();
        target.put("deviceIds", devices.stream().map(ControlProfile::deviceId).toList());
        if (req.spaceId() != null) {
            target.put("spaceId", req.spaceId());
            target.put("includeChildren", req.includeChildren());
        }
        long jobId = repository.insertBulkJob(organizationId, userId, target, req.capability(), req.command(), req.args(), devices.size(),
                clock.instant());
        executor.execute(() -> submitAll(organizationId, userId, jobId, devices.stream().map(ControlProfile::deviceId).toList(),
                req.capability(), req.command(), req.args()));
        return Map.of("bulkJobId", Long.toString(jobId), "total", devices.size());
    }

    /** 기기마다 명령을 낸다(멱등). 끝나면 작업을 COMPLETED로 */
    void submitAll(long organizationId, long userId, long jobId, List<Long> deviceIds, String capability, String command,
                   Map<String, Object> args) {
        CommandSource source = CommandSource.bulk(userId, Long.toString(jobId));
        for (Long deviceId : deviceIds) {
            String key = ActionIdempotencyKeys.of("bulk", Long.toString(jobId), Long.toString(deviceId));
            try {
                facade.submit(new CommandRequest(organizationId, deviceId, capability, command, args, source, null, key, null, null, null,
                        false, false));
            } catch (RuntimeException e) {
                log.warn("일괄 제어 명령 실패 job={} device={}: {}", jobId, deviceId, e.toString());
            }
        }
        Map<String, Integer> c = counts(organizationId, jobId);
        repository.finishBulkJob(organizationId, jobId, c.get("succeeded"), c.get("failed"), c.get("queued"), clock.instant());
    }

    /** 실행 중 파드가 죽어 남은 작업을 이어서 낸다(주기 작업) */
    public int resumeStale() {
        int n = 0;
        for (SceneBulkRepository.BulkJob job : repository.findStaleRunning(clock.instant().minus(Duration.ofMinutes(2)))) {
            List<Long> ids = new ArrayList<>();
            Object raw = job.target().get("deviceIds");
            if (raw instanceof List<?> l) {
                l.forEach(o -> ids.add(Long.parseLong(o.toString())));
            }
            submitAll(job.organizationId(), job.requestedBy(), job.id(), ids, job.capability(), job.command(), job.args());
            n++;
        }
        return n;
    }

    /** 진행(API-ACT-05 {@code GET /command-bulk-jobs/{id}}) */
    public Map<String, Object> progress(long organizationId, long jobId) {
        roleChecker.require(Permission.DEV_READ);
        SceneBulkRepository.BulkJob job = repository.findBulkJob(organizationId, jobId)
                .orElseThrow(() -> new BusinessException(CommonErrorCode.RESOURCE_NOT_FOUND));
        List<Command> list = commands.findBySource(organizationId, "BULK", Long.toString(jobId));
        Map<String, Integer> c = tally(list);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("bulkJobId", Long.toString(jobId));
        out.put("status", job.status());
        out.put("total", job.total());
        out.putAll(c);
        out.put("items", list.stream().map(cmd -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("deviceId", Long.toString(cmd.deviceId()));
            m.put("commandId", cmd.id().toString());
            m.put("status", cmd.status().name());
            if (cmd.statusReason() != null) {
                m.put("reason", cmd.statusReason());
            }
            return m;
        }).toList());
        return out;
    }

    private Map<String, Integer> counts(long organizationId, long jobId) {
        return tally(commands.findBySource(organizationId, "BULK", Long.toString(jobId)));
    }

    /** 상태 묶음: 적용(APPLIED·ACKED·SENT·REQUESTED 진행 포함 안 함), 실패, 대기, 건너뜀 */
    static Map<String, Integer> tally(List<Command> list) {
        int ok = 0;
        int failed = 0;
        int queued = 0;
        int skipped = 0;
        for (Command c : list) {
            CommandStatus s = c.status();
            switch (s) {
                case APPLIED, ACKED -> ok++;
                case FAILED, TIMEOUT, REJECTED, BLOCKED -> failed++;
                case QUEUED, QUEUED_FOR_DOWNLINK, DELAYED -> queued++;
                case SKIPPED, SUPERSEDED, CANCELLED -> skipped++;
                default -> {
                }
            }
        }
        Map<String, Integer> m = new LinkedHashMap<>();
        m.put("succeeded", ok);
        m.put("failed", failed);
        m.put("queued", queued);
        m.put("skipped", skipped);
        return m;
    }

    /** 대상을 펼치고 규칙(BR-ACT-17)을 확인한다 */
    private List<ControlProfile> resolve(long organizationId, BulkRequest req) {
        if (req.capability() == null || req.command() == null) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail(req.capability() == null ? "capability" : "command", "REQUIRED", "필수입니다")));
        }
        LinkedHashSet<Long> ids = new LinkedHashSet<>();
        if (req.deviceIds() != null) {
            ids.addAll(req.deviceIds());
        }
        if (req.spaceId() != null) {
            ids.addAll(core.spaceDevices(req.spaceId(), "controls", req.capability(), req.includeChildren()));
        }
        List<Long> devices = new ArrayList<>(ids);
        if (devices.size() > BulkPolicy.MAX_DEVICES) {
            throw new BusinessException(ActionErrorCode.COMMAND_BULK_LIMIT_EXCEEDED);
        }
        Map<Long, Boolean> supports = new LinkedHashMap<>();
        List<ControlProfile> profilesList = new ArrayList<>();
        for (Long id : devices) {
            Optional<ControlProfile> p = profiles.find(id).filter(x -> x.organizationId() == organizationId);
            supports.put(id, p.map(x -> x.controllable() && x.capability(req.capability()).isPresent()).orElse(false));
            p.ifPresent(profilesList::add);
        }
        BulkPolicy.Check check = BulkPolicy.check(devices, supports);
        if (!check.ok()) {
            if ("CAPABILITY_NOT_SUPPORTED".equals(check.code())) {
                throw new BusinessException(ActionErrorCode.CAPABILITY_NOT_SUPPORTED, check.unsupported().stream()
                        .map(d -> new FieldErrorDetail("target.deviceIds", "CAPABILITY_NOT_SUPPORTED", Long.toString(d))).toList());
            }
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("target", "EMPTY", "대상 기기가 없습니다")));
        }
        for (ControlProfile p : profilesList) {
            roleChecker.require(Permission.DEVICE_CONTROL, p.spaceId(), ActionErrorCode.DEVICE_NOT_FOUND);
        }
        return profilesList;
    }
}
