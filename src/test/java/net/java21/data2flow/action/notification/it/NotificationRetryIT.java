package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 발송 재시도(RUL-03.05, OPS-06.03, BR-RUL-17·BR-OPS-06): 텔레그램 대역의 500·429와 MutableClock */
class NotificationRetryIT extends NotificationITSupport {

    private void sendOne() {
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
    }

    private String status() {
        return jdbc.sql("SELECT status FROM data2flow_action.notification_deliveries").query(String.class).single();
    }

    private int attempt() {
        return jdbc.sql("SELECT attempt FROM data2flow_action.notification_deliveries").query(Integer.class).single();
    }

    private Instant nextRetry() {
        return jdbc.sql("SELECT next_retry_at FROM data2flow_action.notification_deliveries").query(OffsetDateTime.class).single().toInstant();
    }

    @Test
    @DisplayName("[RUL-03.05][AT-RUL-07.2][TC-RUL-080][TC-OPS-064] 500이 계속 → 재시도 예약 30초·2분·10분·30분·1시간 확인 후 FAILED, 이력 attempt=6(첫 시도+5회)·오류 사유, notification.failed")
    void exhausts() {
        for (int i = 0; i < 6; i++) {
            TELEGRAM.nextStatus(500, null);
        }
        sendOne();
        List<Duration> waits = new ArrayList<>();
        while (status().equals("RETRYING")) {
            waits.add(Duration.between(clock.instant(), nextRetry()));
            advanceToNextRetryAndRun();
        }
        assertThat(waits).containsExactly(Duration.ofSeconds(30), Duration.ofMinutes(2), Duration.ofMinutes(10), Duration.ofMinutes(30),
                Duration.ofHours(1));
        assertThat(status()).isEqualTo("FAILED");
        assertThat(attempt()).isEqualTo(6);
        assertThat(jdbc.sql("SELECT last_error FROM data2flow_action.notification_deliveries").query(String.class).single()).contains("500");
        assertThat(outbox("notification.failed")).isEqualTo(1);
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(6);
    }

    @Test
    @DisplayName("[RUL-03.05][TC-RUL-080][TC-OPS-063] 500·500 뒤 3번째에 200 → SENT, attempt=3")
    void recovers() {
        TELEGRAM.nextStatus(500, null);
        TELEGRAM.nextStatus(500, null);
        sendOne();
        advanceToNextRetryAndRun();
        advanceToNextRetryAndRun();
        assertThat(status()).isEqualTo("SENT");
        assertThat(attempt()).isEqualTo(3);
        assertThat(outbox("notification.delivered")).isEqualTo(1);
    }

    @Test
    @DisplayName("[RUL-03.05][TC-RUL-080] 429 retry_after 7 → 7초 뒤 재시도, 그 전에는 다시 보내지 않는다")
    void retryAfter() {
        TELEGRAM.nextStatus(429, 7);
        sendOne();
        assertThat(Duration.between(clock.instant(), nextRetry())).isEqualTo(Duration.ofSeconds(7));
        clock.advanceBy(Duration.ofSeconds(6));
        dispatcher.processDue();
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);
        clock.advanceBy(Duration.ofSeconds(1));
        dispatcher.processDue();
        assertThat(status()).isEqualTo("SENT");
    }

    @Test
    @DisplayName("[OPS-06.03] 영구 실패(403)는 재시도하지 않고 FAILED")
    void permanent() {
        TELEGRAM.nextStatus(403, null);
        sendOne();
        assertThat(status()).isEqualTo("FAILED");
        assertThat(attempt()).isEqualTo(1);
    }
}
