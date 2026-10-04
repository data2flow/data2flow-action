package net.java21.data2flow.action.actuation.controller;

import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.service.BulkControlService;
import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.action.actuation.service.DriverHealthService;
import net.java21.data2flow.action.actuation.service.RuntimeStatsService;
import net.java21.data2flow.action.actuation.service.SceneService;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.SourceType;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.identity.CurrentUser;
import net.java21.data2flow.contracts.identity.CurrentUserHolder;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * action M4 제어 내부 API(ACT-api §5.2): 일괄 제어(API-ACT-05), 장면 실행·미리보기(API-ACT-11·12), 드라이버 지표(API-ACT-32), 가동·효과
 * 현황(API-ACT-35), 인터락 차단 기록(API-ACT-16 blocks). core-api가 권한·공간 범위를 본 뒤 신원 헤더와 함께 넘긴다(ADR-021).
 */
@RestController
public class InternalControlController {

    private final BulkControlService bulk;
    private final SceneService scenes;
    private final DriverHealthService health;
    private final RuntimeStatsService runtime;
    private final CommandRepository commands;
    private final ControlProfileService profiles;
    private final RoleChecker roleChecker;
    private final Clock clock;

    public InternalControlController(BulkControlService bulk, SceneService scenes, DriverHealthService health, RuntimeStatsService runtime,
                                     CommandRepository commands, ControlProfileService profiles, RoleChecker roleChecker, Clock clock) {
        this.bulk = bulk;
        this.scenes = scenes;
        this.health = health;
        this.runtime = runtime;
        this.commands = commands;
        this.profiles = profiles;
        this.roleChecker = roleChecker;
        this.clock = clock;
    }

    /** 일괄 제어 본문(API-ACT-05) */
    public record BulkBody(Target target, String capability, String command, Map<String, Object> args, Boolean preview) {
        /** 대상: 기기 목록 또는 공간(+하위)·기능 */
        public record Target(List<String> deviceIds, String spaceId, Boolean includeChildren, String capability) {
        }
    }

    /** API-ACT-05 일괄 제어: 미리보기는 200, 실행은 202 {bulkJobId, total} */
    @PostMapping("/internal/action/bulk")
    public ResponseEntity<ApiResponse<Map<String, Object>>> bulk(@RequestBody BulkBody body) {
        CurrentUser user = user();
        if (body.target() == null) {
            throw invalid("target", "필수입니다");
        }
        List<Long> ids = new ArrayList<>();
        if (body.target().deviceIds() != null) {
            body.target().deviceIds().forEach(id -> ids.add(parseId(id, "target.deviceIds")));
        }
        Long spaceId = body.target().spaceId() == null ? null : parseId(body.target().spaceId(), "target.spaceId");
        String capability = body.capability() == null ? body.target().capability() : body.capability();
        var req = new BulkControlService.BulkRequest(ids, spaceId, Boolean.TRUE.equals(body.target().includeChildren()), capability,
                body.command(), body.args() == null ? Map.of() : body.args());
        if (Boolean.TRUE.equals(body.preview())) {
            return ResponseEntity.ok(ApiResponse.success(bulk.preview(user.organizationId(), req)));
        }
        return ResponseEntity.status(202).body(ApiResponse.success(bulk.start(user.organizationId(), user.userId(), req)));
    }

    /** API-ACT-05 일괄 작업 진행 */
    @GetMapping("/internal/action/bulk-jobs/{bulk-job-id}")
    public ApiResponse<Map<String, Object>> bulkJob(@PathVariable("bulk-job-id") long jobId) {
        return ApiResponse.success(bulk.progress(user().organizationId(), jobId));
    }

    /** 장면 실행 본문(API-ACT-11): 출처가 없으면 요청 사용자(MANUAL) */
    public record SceneRunBody(String priority, Map<String, Object> source) {
    }

    /** API-ACT-11 장면 실행 → 202 {sceneRunId}. 같은 Idempotency-Key는 기기마다 한 번만 */
    @PostMapping("/internal/action/scenes/{scene-id}/run")
    public ResponseEntity<ApiResponse<Map<String, Object>>> runScene(@PathVariable("scene-id") long sceneId,
                                                                     @RequestBody(required = false) SceneRunBody body,
                                                                     @RequestHeader(value = DataflowHeaders.IDEMPOTENCY_KEY, required = false) String key) {
        CurrentUser user = user();
        roleChecker.require(Permission.SCENE_RUN);
        CommandSource origin = CommandSource.user(user.userId());
        if (body != null && body.source() != null && body.source().get("type") != null) {
            SourceType type = SourceType.valueOf(body.source().get("type").toString());
            if (type == SourceType.SCHEDULE && body.source().get("scheduleId") != null) {
                origin = CommandSource.schedule(Long.parseLong(body.source().get("scheduleId").toString()));
            } else if (type == SourceType.AI && body.source().get("suggestionId") != null && body.source().get("approvedBy") != null) {
                origin = CommandSource.ai(body.source().get("suggestionId").toString(), Long.parseLong(body.source().get("approvedBy").toString()));
            }
        }
        String runKey = key == null || key.isBlank() ? java.util.UUID.randomUUID().toString()
                : ActionIdempotencyKeys.of("scene-run", Long.toString(user.userId()), key);
        long runId = scenes.run(user.organizationId(), sceneId, origin, runKey);
        return ResponseEntity.status(202).body(ApiResponse.success(Map.of("sceneRunId", Long.toString(runId))));
    }

    /** API-ACT-12 장면 미리보기 */
    @PostMapping("/internal/action/scenes/{scene-id}/preview")
    public ApiResponse<Map<String, Object>> previewScene(@PathVariable("scene-id") long sceneId) {
        CurrentUser user = user();
        roleChecker.require(Permission.DEV_READ);
        return ApiResponse.success(scenes.preview(user.organizationId(), sceneId));
    }

    /** API-ACT-11 장면 실행 결과 */
    @GetMapping("/internal/action/scene-runs/{scene-run-id}")
    public ApiResponse<Map<String, Object>> sceneRun(@PathVariable("scene-run-id") long runId) {
        CurrentUser user = user();
        roleChecker.require(Permission.DEV_READ);
        return ApiResponse.success(scenes.refresh(user.organizationId(), runId));
    }

    /** API-ACT-32 드라이버 지표(window=1h|24h) */
    @GetMapping("/internal/action/drivers/{driver-id}/metrics")
    public ApiResponse<Map<String, Object>> driverMetrics(@PathVariable("driver-id") long driverId,
                                                          @RequestParam(required = false) String window) {
        CurrentUser user = user();
        roleChecker.require(Permission.DRIVER_MANAGE);
        Duration w = "24h".equalsIgnoreCase(window) ? Duration.ofHours(24) : Duration.ofHours(1);
        return ApiResponse.success(health.metrics(user.organizationId(), driverId, w));
    }

    /** API-ACT-35 가동·효과 현황(from·to는 날짜, 기본 최근 7일) */
    @GetMapping("/internal/action/devices/{device-id}/runtime")
    public ApiResponse<Map<String, Object>> runtime(@PathVariable("device-id") long deviceId,
                                                    @RequestParam(required = false) LocalDate from,
                                                    @RequestParam(required = false) LocalDate to) {
        CurrentUser user = user();
        var p = profiles.find(deviceId).filter(x -> x.organizationId() == user.organizationId())
                .orElseThrow(() -> new BusinessException(ActionErrorCode.DEVICE_NOT_FOUND));
        roleChecker.require(Permission.DEV_READ, p.spaceId(), ActionErrorCode.DEVICE_NOT_FOUND);
        LocalDate end = to == null ? LocalDate.ofInstant(clock.instant(), java.time.ZoneOffset.UTC) : to;
        LocalDate start = from == null ? end.minusDays(6) : from;
        return ApiResponse.success(runtime.runtime(user.organizationId(), deviceId, start, end));
    }

    /** API-ACT-16 인터락 차단 기록(최근 200건) */
    @GetMapping("/internal/action/interlocks/{interlock-id}/blocks")
    public ApiResponse<Map<String, Object>> interlockBlocks(@PathVariable("interlock-id") long interlockId,
                                                            @RequestParam(required = false) Instant from,
                                                            @RequestParam(required = false) Instant to) {
        CurrentUser user = user();
        roleChecker.require(Permission.INTERLOCK_MANAGE);
        Instant end = to == null ? clock.instant() : to;
        Instant start = from == null ? end.minus(Duration.ofDays(7)) : from;
        return ApiResponse.success(Map.of("responses", commands.findInterlockBlocks(user.organizationId(), interlockId, start, end, 200)));
    }

    private static CurrentUser user() {
        return CurrentUserHolder.find().orElseThrow(() -> new BusinessException(CommonErrorCode.AUTH_TOKEN_INVALID));
    }

    private static long parseId(String value, String field) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw invalid(field, "숫자 ID여야 합니다");
        }
    }

    private static BusinessException invalid(String field, String message) {
        return new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail(field, "INVALID", message)));
    }
}
