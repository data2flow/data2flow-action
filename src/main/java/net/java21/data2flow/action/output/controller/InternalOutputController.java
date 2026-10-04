package net.java21.data2flow.action.output.controller;

import net.java21.data2flow.action.output.service.OutputDeliveryService;
import net.java21.data2flow.action.output.service.OutputTestService;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.web.ApiResponse;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

/**
 * 출력 연결 내부 API(core-api → action, ADR-021). 외부 API-DSC-32·33({@code /api/v1/core/output-connections/**})은 core가 권한
 * (SRC_ADMIN)을 본 뒤 여기로 넘긴다. 조직은 본문 {@code organizationId}.
 * <ul>
 *   <li>API-DSC-76 {@code POST /internal/action/output-connections/test}: 샘플 하나를 실제로 보낸다. 실패도 200 {@code ok=false}</li>
 *   <li>API-DSC-77 {@code POST /internal/action/output-connections/{output-connection-id}/replay-failed}: 실패 보관함 재전송 → {@code {queued}}</li>
 * </ul>
 */
@RestController
public class InternalOutputController {

    private final OutputTestService tests;
    private final OutputDeliveryService deliveries;

    public InternalOutputController(OutputTestService tests, OutputDeliveryService deliveries) {
        this.tests = tests;
        this.deliveries = deliveries;
    }

    @PostMapping("/internal/action/output-connections/test")
    public ApiResponse<Map<String, Object>> test(@RequestBody JsonNode body) {
        require(body, "organizationId");
        require(body, "type");
        require(body, "target");
        require(body, "sample");
        try {
            return ApiResponse.success(tests.test(body));
        } catch (IllegalArgumentException | tools.jackson.core.JacksonException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("body", "INVALID", e.getMessage())));
        }
    }

    @PostMapping("/internal/action/output-connections/{output-connection-id}/replay-failed")
    public ApiResponse<Map<String, Object>> replayFailed(@PathVariable("output-connection-id") long outputId, @RequestBody JsonNode body) {
        JsonNode orgNode = require(body, "organizationId");
        long org = orgNode.isNumber() ? orgNode.asLong() : parseId(orgNode.asString());
        Instant from = time(body, "from");
        Instant to = time(body, "to");
        if (!from.isBefore(to)) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("to", "RANGE", "to는 from보다 뒤여야 합니다")));
        }
        return ApiResponse.success(Map.of("queued", deliveries.replayFailed(org, outputId, from, to)));
    }

    private static JsonNode require(JsonNode body, String field) {
        JsonNode n = body == null ? null : body.get(field);
        if (n == null || n.isNull() || (n.isString() && n.asString().isBlank())) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail(field, "REQUIRED", "필수입니다")));
        }
        return n;
    }

    private static long parseId(String raw) {
        try {
            return Long.parseLong(raw.strip());
        } catch (NumberFormatException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("organizationId", "INVALID", "숫자 ID여야 합니다")));
        }
    }

    private static Instant time(JsonNode body, String field) {
        try {
            return Instant.parse(require(body, field).asString());
        } catch (DateTimeParseException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail(field, "INVALID", "ISO-8601 시각")));
        }
    }
}
