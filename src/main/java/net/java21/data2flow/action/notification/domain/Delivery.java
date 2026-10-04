package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.notification.DeliveryStatus;

import java.time.Instant;
import java.util.UUID;

/**
 * 발송 한 건(data2flow_action.notification_deliveries). 수신자 한 명·채널 하나에 하나이고 멱등 키로 한 번만 만든다(BR-RUL-17).
 *
 * @param channelId   채널 ID. 웹 알림(WEB)은 0
 * @param aggregateId 묶음 대기·요약이면 묶음 ID
 * @param nextRetryAt PENDING·RETRYING의 다음 호출 시각. 묶음·방해 금지로 대기 중이면 null
 * @param attempt     채널 호출 횟수(첫 시도 포함)
 */
public record Delivery(UUID id, long organizationId, String idempotencyKey, long channelId, String channelType, String sourceType,
                       String sourceId, Long alarmId, Long aggregateId, String recipientKey, Long userId, String requestKey, String event,
                       String severity, Integer stepNo, MessagePayload payload, int digestCount, DeliveryStatus status,
                       String skipReason, int attempt, Instant nextRetryAt, String lastError, String externalMessageId,
                       Instant createdAt, Instant sentAt) {

    public static final String WEB = "WEB";

    public boolean web() {
        return WEB.equals(channelType);
    }
}
