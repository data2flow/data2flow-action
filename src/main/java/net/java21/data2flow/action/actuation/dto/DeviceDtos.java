package net.java21.data2flow.action.actuation.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 기기 상태 쌍·제어 정보 응답(API-ACT-03·04) */
public final class DeviceDtos {

    private DeviceDtos() {
    }

    /** API-ACT-04 {@code {desired, desiredVersion, desiredSource, reported, reportedVersion, reportedAt, delta, connectivity}} */
    public record ShadowResponse(Map<String, Map<String, Object>> desired, long desiredVersion, CommandDtos.SourceView desiredSource,
                                 Map<String, Map<String, Object>> reported, long reportedVersion, Instant reportedAt,
                                 Map<String, Map<String, Object>> delta, String connectivity) {
    }

    /** API-ACT-03 */
    public record ControlResponse(boolean controllable, DriverView driver, List<CapabilityView> capabilities, ShadowResponse shadow,
                                  ManualOverrideView manualOverride, ProtectionView protection, List<PendingView> pending,
                                  Object emergencyStop) {
    }

    /** 드라이버 요약. 이름은 core가 붙인다 */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record DriverView(String id, String name, String type, String status) {
    }

    /** 기능별 컨트롤 생성 정보: 정의 + effectiveConstraints(모델 제약 ∩ 조직 한계) */
    public record CapabilityView(String name, int version, Object attributes, Object commands, Map<String, Object> effectiveConstraints) {
    }

    /** 수동 우선(ACT-06.05): 남은 시간 {@code remainingSeconds}를 함께 준다(기기 카드 "n분 남음", TC-ACT-120) */
    public record ManualOverrideView(String capability, Instant until, String setBy, long remainingSeconds) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ProtectionView(Instant nextAllowedAt) {
    }

    public record PendingView(String commandId, String capability, String command, String status) {
    }

    /** API-ACT-31 연결 확인 요청(core가 드라이버 정의를 넘긴다, 비밀값 없음) */
    /** 연결 확인 본문(API-ACT-31): core가 드라이버 종류·설정과 복호화한 비밀값(선택)을 넘긴다 */
    public record HealthcheckRequest(String type, Map<String, Object> config,
                                     Map<String, net.java21.data2flow.contracts.secret.Secret> secrets) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record HealthcheckResponse(boolean ok, long latencyMs, Set<String> capabilities, ErrorView error) {
    }

    public record ErrorView(String kind, String message) {
    }
}
