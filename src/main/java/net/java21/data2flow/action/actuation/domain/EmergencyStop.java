package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;

import java.time.Instant;

/**
 * 진행 중인 비상 정지(core {@code emergency_stops}, API-ACT-46). 범위 판정은 contracts {@link EmergencyStopChanged.Scope}(BR-ACT-12).
 *
 * @param emergencyStopId ID
 * @param organizationId  조직
 * @param scope           범위
 * @param reason          사유
 * @param startedAt       시작 시각
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EmergencyStop(long emergencyStopId, long organizationId, EmergencyStopChanged.Scope scope, String reason, Instant startedAt) {
}
