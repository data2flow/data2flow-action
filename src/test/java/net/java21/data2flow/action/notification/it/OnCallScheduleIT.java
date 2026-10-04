package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 당직(RUL-05.03, BR-RUL-19) */
class OnCallScheduleIT extends NotificationITSupport {

    @Test
    @DisplayName("[RUL-05.03][AT-RUL-11.1][TC-RUL-098] 평일 18-09 당직 A(5), 오늘 대체자 B(6)에서 21시 알람 → B에게 발송")
    void substitute() {
        List<Map<String, Object>> shifts = new ArrayList<>();
        for (int d = 1; d <= 5; d++) {
            shifts.add(Map.of("dayOfWeek", d, "from", "18:00", "to", "09:00", "userId", "5"));
        }
        fx.onCall = Map.of("timezone", "Asia/Seoul", "shifts", shifts, "overrides", List.of(Map.of("startsAt", "2026-03-02T09:00:00Z",
                "endsAt", "2026-03-03T00:00:00Z", "originalUserId", "5", "substituteUserId", "6")));
        clock.set(Instant.parse("2026-03-02T12:00:00Z"));   // 월 21:00
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.onCall("TELEGRAM")), null));
        assertThat(TELEGRAM.calls("sendMessage")).singleElement()
                .satisfies(c -> assertThat(c.body().path("chat_id").asString()).isEqualTo("666"));
    }

    @Test
    @DisplayName("[RUL-05.03][AT-RUL-11.2][TC-RUL-099] 당직 비어 있음 → 정책의 다른 수신자(7)에게 발송, 다른 수신자도 없으면 조직 ADMIN")
    void emptyFallsBack() {
        clock.set(Instant.parse("2026-03-02T12:00:00Z"));
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.onCall("TELEGRAM"), NotificationRecipient.user(6, "TELEGRAM")), null));
        assertThat(TELEGRAM.calls("sendMessage")).extracting(c -> c.body().path("chat_id").asString()).containsExactly("666");

        fx.alarms.put(9002L, NotificationFixtures.alarm(9002, "MAJOR", "ACTIVE"));
        notifications.handle(alarmRequest(9002, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.onCall("TELEGRAM")), null));
        assertThat(TELEGRAM.calls("sendMessage")).extracting(c -> c.body().path("chat_id").asString()).containsExactly("666", "777");
    }
}
