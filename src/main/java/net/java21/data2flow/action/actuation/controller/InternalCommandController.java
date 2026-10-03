package net.java21.data2flow.action.actuation.controller;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.dto.CommandDtos.CommandResponse;
import net.java21.data2flow.action.actuation.dto.CommandDtos.SubmitCommandRequest;
import net.java21.data2flow.action.actuation.dto.DeviceDtos;
import net.java21.data2flow.action.actuation.repository.CommandEventRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository.CommandQuery;
import net.java21.data2flow.action.actuation.service.CommandQueryService;
import net.java21.data2flow.action.actuation.service.CommandRequest;
import net.java21.data2flow.action.actuation.service.CommandWaiter;
import net.java21.data2flow.action.actuation.service.ControlFacade;
import net.java21.data2flow.action.actuation.service.IdempotencyKeys;
import net.java21.data2flow.action.actuation.service.Outcome;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.SourceType;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.identity.CurrentUser;
import net.java21.data2flow.contracts.identity.CurrentUserHolder;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.contracts.web.CursorListApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * action 내부 API(ACT-api §5.2, ADR-021: ClusterIP·토큰 없음, core-api가 {@code X-USER-ID}·{@code X-ORG-ID}·{@code X-CALLER-SERVICE}를 넘긴다).
 * 외부 경로 {@code /api/v1/core/devices/{device-id}/commands} 등은 core-api가 권한·공간 범위를 본 뒤 여기로 넘긴다.
 */
@RestController
public class InternalCommandController {

    private final ControlFacade facade;
    private final CommandQueryService queries;
    private final CommandRepository commands;
    private final CommandEventRepository timeline;
    private final CommandWaiter waiter;
    private final ActionProperties properties;
    private final Clock clock;

    public InternalCommandController(ControlFacade facade, CommandQueryService queries, CommandRepository commands,
                                     CommandEventRepository timeline, CommandWaiter waiter, ActionProperties properties, Clock clock) {
        this.facade = facade;
        this.queries = queries;
        this.commands = commands;
        this.timeline = timeline;
        this.waiter = waiter;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * API-ACT-01 사용자 명령: 202(바로 반환) 또는 200({@code wait=ack|applied}가 10초 안에 이루어짐). 거부는 4xx + {@code response.commandId},
     * AUTO·SCHEDULE·AI 출처의 최소 간격 위반은 429 {@code COMMAND_RATE_LIMITED} + {@code Retry-After}.
     */
    @PostMapping("/internal/action/commands")
    public ResponseEntity<ApiResponse<CommandResponse>> submit(@RequestBody SubmitCommandRequest body,
                                                               @RequestHeader(value = DataflowHeaders.IDEMPOTENCY_KEY, required = false) String header) {
        CurrentUser user = user();
        String key = body.idempotencyKey() == null ? header : body.idempotencyKey();
        if (!ActionIdempotencyKeys.isValid(key)) {
            throw invalid("Idempotency-Key", "1~64자, 허용 문자 A-Z a-z 0-9 . _ : -");
        }
        if (body.deviceId() == null || body.capability() == null || body.command() == null) {
            throw invalid(body.deviceId() == null ? "deviceId" : body.capability() == null ? "capability" : "command", "필수입니다");
        }
        long deviceId = parseId(body.deviceId(), "deviceId");
        CommandSource source = body.source() == null ? CommandSource.user(user.userId()) : body.source().toSource();
        if ((source.type() == SourceType.USER || source.type() == SourceType.BULK) && source.userId() != null
                && source.userId() != user.userId()) {
            throw invalid("source.userId", "요청 사용자와 다릅니다");
        }
        if (source.type() == SourceType.USER && source.userId() == null) {
            source = CommandSource.user(user.userId());
        }
        boolean userCommand = source.type() == SourceType.USER || source.type() == SourceType.BULK;
        String storedKey = userCommand ? IdempotencyKeys.user(user.userId(), key) : IdempotencyKeys.action(source.type(), key);
        Long sourceSpace = body.sourceSpaceId() == null ? null : parseId(body.sourceSpaceId(), "sourceSpaceId");
        CommandRequest req = new CommandRequest(user.organizationId(), deviceId, body.capability(), body.command(),
                body.args() == null ? Map.of() : body.args(), source, body.priority(), storedKey, null, body.validitySeconds(), sourceSpace,
                userCommand, false);
        Outcome outcome = facade.submit(req);
        String wait = body.waitFor() == null ? "none" : body.waitFor().toLowerCase(Locale.ROOT);
        Command c = outcome.command();
        if (outcome.rejection() != null) {
            boolean blockedAccepted = c.status() == CommandStatus.BLOCKED && "none".equals(wait);
            if (!blockedAccepted) {
                throw new CommandRejectedException(outcome.rejection(), c);
            }
        }
        boolean reached = false;
        if (!"none".equals(wait) && outcome.rejection() == null) {
            c = awaitStatus(c, "applied".equals(wait));
            reached = reachedTarget(c, "applied".equals(wait));
        }
        CommandResponse response = CommandResponse.of(c, timeline.findByCommand(c.organizationId(), c.id()), outcome.message());
        return ResponseEntity.status(reached ? 200 : 202).body(ApiResponse.success(response));
    }

    /** API-ACT-02 명령 상세 */
    @GetMapping("/internal/action/commands/{command-id}")
    public ApiResponse<CommandResponse> get(@PathVariable("command-id") String commandId) {
        return ApiResponse.success(queries.get(user().organizationId(), parseCommandId(commandId)));
    }

    /** API-ACT-02 취소(QUEUED·DELAYED·QUEUED_FOR_DOWNLINK만) */
    @PostMapping("/internal/action/commands/{command-id}/cancel")
    public ApiResponse<CommandResponse> cancel(@PathVariable("command-id") String commandId) {
        return ApiResponse.success(queries.cancel(user().organizationId(), parseCommandId(commandId)));
    }

    /** API-ACT-02 기기별 명령 이력(커서 목록, 최신순) */
    @GetMapping("/internal/action/devices/{device-id}/commands")
    public CursorListApiResponse<CommandResponse> history(@PathVariable("device-id") long deviceId,
                                                          @RequestParam(required = false) Instant from,
                                                          @RequestParam(required = false) Instant to,
                                                          @RequestParam(required = false) String sourceType,
                                                          @RequestParam(required = false) String status,
                                                          @RequestParam(required = false) String capability,
                                                          @RequestParam(required = false) String cursor,
                                                          @RequestParam(required = false) Integer size) {
        CommandQuery q = new CommandQuery(user().organizationId(), deviceId, from, to, upper(sourceType), upper(status),
                capability);
        return queries.list(q, cursor, size);
    }

    /** API-ACT-04 상태 쌍 */
    @GetMapping("/internal/action/devices/{device-id}/shadow")
    public ApiResponse<DeviceDtos.ShadowResponse> shadow(@PathVariable("device-id") long deviceId) {
        return ApiResponse.success(queries.shadow(user().organizationId(), deviceId));
    }

    /** API-ACT-03 제어 정보 */
    @GetMapping("/internal/action/devices/{device-id}/control")
    public ApiResponse<DeviceDtos.ControlResponse> control(@PathVariable("device-id") long deviceId) {
        return ApiResponse.success(queries.control(user().organizationId(), deviceId));
    }

    /** API-ACT-06 수동 우선 해제 → 204 */
    @DeleteMapping("/internal/action/devices/{device-id}/manual-override")
    public ResponseEntity<Void> releaseManualOverride(@PathVariable("device-id") long deviceId,
                                                      @RequestParam(required = false) String capability) {
        queries.releaseManualOverride(user().organizationId(), deviceId, capability);
        return ResponseEntity.noContent().build();
    }

    /** API-ACT-31 드라이버 연결 확인(core가 종류·설정을 넘긴다). 실패면 502 DRIVER_HEALTHCHECK_FAILED */
    @PostMapping("/internal/action/drivers/{driver-id}/healthcheck")
    public ApiResponse<DeviceDtos.HealthcheckResponse> healthcheck(@PathVariable("driver-id") long driverId,
                                                                   @RequestBody DeviceDtos.HealthcheckRequest body) {
        return ApiResponse.success(queries.healthcheck(driverId, body));
    }

    /** core가 넘긴 신원(X-USER-ID·X-ORG-ID). 없으면 401 */
    private static CurrentUser user() {
        return CurrentUserHolder.find().orElseThrow(() -> new BusinessException(CommonErrorCode.AUTH_TOKEN_INVALID));
    }

    private Command awaitStatus(Command c, boolean applied) {
        Instant deadline = clock.instant().plus(properties.command().waitMax());
        Command current = c;
        while (!reachedTarget(current, applied) && !current.status().terminal()) {
            Duration left = Duration.between(clock.instant(), deadline);
            if (left.isNegative() || left.isZero()) {
                break;
            }
            waiter.awaitSignal(current.id(), left.compareTo(Duration.ofMillis(200)) < 0 ? left : Duration.ofMillis(200));
            current = commands.findByIdAndOrganizationId(c.id(), c.organizationId()).orElse(current);
        }
        return current;
    }

    private static boolean reachedTarget(Command c, boolean applied) {
        if (applied) {
            return c.status() == CommandStatus.APPLIED;
        }
        return c.status() == CommandStatus.ACKED || c.status() == CommandStatus.APPLIED;
    }

    private static String upper(String s) {
        return s == null || s.isBlank() ? null : s.toUpperCase(Locale.ROOT);
    }

    private static UUID parseCommandId(String id) {
        try {
            return UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            throw new BusinessException(ActionErrorCode.COMMAND_NOT_FOUND);
        }
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
