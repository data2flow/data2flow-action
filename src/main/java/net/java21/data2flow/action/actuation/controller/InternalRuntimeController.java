package net.java21.data2flow.action.actuation.controller;

import net.java21.data2flow.action.actuation.service.RuntimeStatsService;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.web.ApiResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 가동 집계 서비스 간 내부 API(ADR-021: 토큰·사용자 없음, 호출자 표시 X-CALLER-SERVICE). analytics 에너지 일 집계(ENE-01, BR-ENE-04)가 추정 원천의
 * 하루 가동 시간을 읽는다. 조직은 쿼리 {@code orgId}.
 * <ul>
 *   <li>{@code GET /internal/action/runtime/daily?orgId=&date=&deviceIds=1,2} → {@code {date, timezone, items:[{deviceId, onSeconds}]}}.
 *       집계 행이 없는 기기는 빠진다(가동 기록 없음)</li>
 * </ul>
 */
@RestController
public class InternalRuntimeController {

    /** 한 번에 물을 수 있는 기기 수 */
    static final int MAX_DEVICES = 500;

    private final RuntimeStatsService runtime;

    public InternalRuntimeController(RuntimeStatsService runtime) {
        this.runtime = runtime;
    }

    @GetMapping("/internal/action/runtime/daily")
    public ApiResponse<Map<String, Object>> daily(@RequestParam long orgId, @RequestParam LocalDate date, @RequestParam String deviceIds) {
        List<Long> ids;
        try {
            ids = Arrays.stream(deviceIds.split(",")).map(String::strip).filter(s -> !s.isEmpty()).map(Long::parseLong).distinct().toList();
        } catch (NumberFormatException e) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("deviceIds", "INVALID", "쉼표로 나눈 기기 ID")));
        }
        if (ids.size() > MAX_DEVICES) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("deviceIds", "SIZE", "최대 " + MAX_DEVICES + "개")));
        }
        return ApiResponse.success(runtime.onSeconds(orgId, date, ids));
    }
}
