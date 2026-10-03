package net.java21.data2flow.action.actuation.driver;

import java.time.Instant;
import java.util.Map;

/**
 * 드라이버가 읽은 기기 상태(ACT-api §5.3). 버전이 이전보다 커야 반영된다(BR-ACT-05).
 *
 * @param deviceExternalId 외부 ID
 * @param capabilities     {@code {capability:{attr:value}}}
 * @param version          보고 버전(시퀀스 또는 보고 시각 기반)
 * @param reportedAt       보고 시각
 */
public record ReportedState(String deviceExternalId, Map<String, Map<String, Object>> capabilities, long version,
                            Instant reportedAt) {
}
