package net.java21.data2flow.action.sink.controller;

import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkTestResult;
import net.java21.data2flow.action.sink.connector.TargetSchema;
import net.java21.data2flow.action.sink.service.SinkConnectionService;
import net.java21.data2flow.action.sink.service.SinkConnectionService.DeadLetter;
import net.java21.data2flow.action.sink.service.SinkConnectionService.SchemaCheck;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.identity.CurrentUser;
import net.java21.data2flow.contracts.identity.CurrentUserHolder;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.web.ApiResponse;
import net.java21.data2flow.contracts.web.CursorListApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sink 내부 API(core-api → action, ADR-021). 외부 API-FLW-50·51({@code /api/v1/core/sink-connections/**})은 core가 권한
 * (SINK_CONNECTION_MANAGE·FLOW_READ)을 본 뒤 여기로 넘긴다. 신원 헤더({@code X-USER-ID}·{@code X-ORG-ID})가 없으면 401.
 *
 * <ul>
 *   <li>{@code POST /internal/action/sinks/connections/test} 저장 전 연결 테스트(본문 {type, config, secrets})</li>
 *   <li>{@code POST /internal/action/sinks/connections/{connection-id}/test} 저장된 연결 테스트</li>
 *   <li>{@code GET|POST /internal/action/sinks/connections/{connection-id}/schema} 대상 확인·자동 생성(FLW-04.04)</li>
 *   <li>{@code GET /internal/action/sinks/connections/{connection-id}/dead-letters} 커서 목록,
 *       {@code POST …/dead-letters/resend} 재전송(BR-FLW-28)</li>
 * </ul>
 * 연결 테스트는 실패해도 200 {@code {ok:false, error:{kind, message}}}로 돌려준다(core가 502 SINK_CONNECTION_TEST_FAILED로 바꾼다).
 */
@RestController
public class InternalSinkController {

    private final SinkConnectionService service;

    public InternalSinkController(SinkConnectionService service) {
        this.service = service;
    }

    @PostMapping("/internal/action/sinks/connections/test")
    public ApiResponse<TestResponse> testDraft(@RequestBody DraftRequest body) {
        CurrentUser user = user();
        if (body == null || body.type() == null || body.type().isBlank()) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("type", "REQUIRED", "필수입니다")));
        }
        Map<String, Secret> secrets = new LinkedHashMap<>();
        if (body.secrets() != null) {
            body.secrets().forEach((k, v) -> {
                if (v != null) {
                    secrets.put(k, Secret.of(v));
                }
            });
        }
        SinkConnection draft = new SinkConnection(0, user.organizationId(), body.type(), body.config(), secrets, 0);
        return ApiResponse.success(TestResponse.of(service.testDraft(draft)));
    }

    @PostMapping("/internal/action/sinks/connections/{connection-id}/test")
    public ApiResponse<TestResponse> test(@PathVariable("connection-id") long connectionId) {
        return ApiResponse.success(TestResponse.of(service.test(user().organizationId(), connectionId)));
    }

    @GetMapping("/internal/action/sinks/connections/{connection-id}/schema")
    public ApiResponse<SchemaCheck> schema(@PathVariable("connection-id") long connectionId, @RequestParam String target,
                                           @RequestParam(required = false) String columns) {
        List<String> cols = columns == null || columns.isBlank() ? List.of()
                : Arrays.stream(columns.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
        return ApiResponse.success(service.schema(user().organizationId(), connectionId, target, cols));
    }

    @PostMapping("/internal/action/sinks/connections/{connection-id}/schema")
    public ApiResponse<SchemaCheck> create(@PathVariable("connection-id") long connectionId, @RequestBody CreateRequest body) {
        if (body == null || body.target() == null || body.columns() == null || body.columns().isEmpty()) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("columns", "REQUIRED", "target과 columns가 필요합니다")));
        }
        return ApiResponse.success(service.create(user().organizationId(), connectionId, body.target(), body.columns(),
                body.primaryKey()));
    }

    @GetMapping("/internal/action/sinks/connections/{connection-id}/dead-letters")
    public CursorListApiResponse<DeadLetter> deadLetters(@PathVariable("connection-id") long connectionId,
                                                         @RequestParam(required = false) String cursor,
                                                         @RequestParam(required = false) Integer size) {
        return service.deadLetters(user().organizationId(), connectionId, cursor, size);
    }

    @PostMapping("/internal/action/sinks/connections/{connection-id}/dead-letters/resend")
    public ApiResponse<ResendResponse> resend(@PathVariable("connection-id") long connectionId, @RequestBody ResendRequest body) {
        return ApiResponse.success(new ResendResponse(service.resend(user().organizationId(), connectionId,
                body == null ? null : body.ids(), body != null && Boolean.TRUE.equals(body.all()))));
    }

    private static CurrentUser user() {
        return CurrentUserHolder.find().orElseThrow(() -> new BusinessException(CommonErrorCode.AUTH_TOKEN_INVALID));
    }

    /** 저장 전 연결 테스트 본문 */
    public record DraftRequest(String type, Map<String, Object> config, Map<String, String> secrets) {
    }

    /** 자동 생성 본문 */
    public record CreateRequest(String target, List<TargetSchema.Column> columns, List<String> primaryKey) {
    }

    /** 재전송 본문 */
    public record ResendRequest(List<String> ids, Boolean all) {
    }

    public record ResendResponse(int resent) {
    }

    /** API-FLW-51 응답 */
    public record TestResponse(boolean ok, long latencyMs, ErrorBody error) {
        static TestResponse of(SinkTestResult r) {
            return new TestResponse(r.ok(), r.latencyMs(), r.ok() ? null : new ErrorBody(r.errorKind().name(), r.message()));
        }
    }

    public record ErrorBody(String kind, String message) {
    }
}
