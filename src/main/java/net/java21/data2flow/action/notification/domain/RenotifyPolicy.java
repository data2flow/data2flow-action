package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.notification.DeliverySkipReasons;
import net.java21.data2flow.contracts.notification.NotificationEvents;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * 재알림(BR-RUL-13, TC-RUL-068·070): 같은 알람의 재발생 알림은 마지막 발송(같은 수신자·채널)에서 재알림 간격이 지나야 다시 보낸다.
 * 해제 알림은 정책의 {@code notifyOnClear}일 때만 보낸다(그 판단은 발송을 만들지 않는 것으로, {@link #sendClear}).
 */
public final class RenotifyPolicy {

    private RenotifyPolicy() {
    }

    /** 건너뛸 사유. 보내야 하면 빈 값 */
    public static Optional<String> skip(String event, Instant lastSentAt, Instant now, Duration renotify) {
        if (!NotificationEvents.ALARM_RERAISED.equals(event) || lastSentAt == null) {
            return Optional.empty();
        }
        return now.isBefore(lastSentAt.plus(renotify)) ? Optional.of(DeliverySkipReasons.RENOTIFY_INTERVAL) : Optional.empty();
    }

    /** 해제 알림을 보낼지(정책이 없으면 요청을 만든 core의 판단을 따른다) */
    public static boolean sendClear(String event, PolicyDefinition policy) {
        return !NotificationEvents.ALARM_CLEARED.equals(event) || policy == null || policy.notifyOnClear();
    }
}
