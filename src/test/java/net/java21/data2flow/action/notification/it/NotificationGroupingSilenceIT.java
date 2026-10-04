package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 묶음·한도·무음(RUL-03.06, RUL-02.07, OPS-06.04) */
class NotificationGroupingSilenceIT extends NotificationITSupport {

    private void alarms(int n, Integer window) {
        for (int i = 0; i < n; i++) {
            long id = 9100 + i;
            fx.alarms.put(id, NotificationFixtures.alarm(id, "MAJOR", "ACTIVE"));
            notifications.handle(alarmRequest(id, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, window,
                    List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
            clock.advanceBy(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("[RUL-03.06][AT-RUL-09.1][TC-RUL-082] 묶기 2분 정책, 1분 안 같은 규칙 알람 7건 → 창이 끝날 때 요약 1건, 이력에 묶인 알람 7건 연결(DIGESTED)")
    void explicitWindow() {
        alarms(7, 120);
        assertThat(TELEGRAM.calls("sendMessage")).isEmpty();
        clock.advanceBy(Duration.ofMinutes(2));
        assertThat(aggregator.flushDue()).isEqualTo(1);
        assertThat(TELEGRAM.calls("sendMessage")).singleElement()
                .satisfies(c -> assertThat(c.body().path("text").asString()).contains("같은 알람 7건"));
        assertThat(deliveries("status = 'DIGESTED'")).isEqualTo(7);
        assertThat(deliveries("status = 'SENT' AND digest_count = 7")).isEqualTo(1);
        assertThat(count("SELECT count(DISTINCT aggregate_id) FROM data2flow_action.notification_deliveries")).isEqualTo(1);
    }

    @Test
    @DisplayName("[OPS-06.04][AT-OPS-12.3][TC-OPS-065] 채널 기본 묶음(60초): 1분 안 같은 알람 12건 → 앞 4건은 바로, 나머지 8건은 창 끝에 요약 1건(버리지 않음)")
    void channelDigest() {
        alarms(12, null);
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(4);
        clock.advanceBy(Duration.ofSeconds(60));
        aggregator.flushDue();
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(5);
        assertThat(TELEGRAM.calls("sendMessage").get(4).body().path("text").asString()).contains("같은 알람 8건");
        assertThat(deliveries("status = 'DIGESTED'")).isEqualTo(8);
    }

    @Test
    @DisplayName("[OPS-06.04][TC-OPS-067] 채널 분당 한도 3건: 서로 다른 규칙 알람 5건 → 3건 바로, 넘친 2건은 요약 1건(버리지 않음)")
    void rateLimit() {
        fx.channels.put(NotificationFixtures.TELEGRAM_CHANNEL, NotificationFixtures.telegramChannel(NotificationFixtures.TELEGRAM_CHANNEL, 3, 60));
        coreCaches().invalidateAll();
        for (int i = 0; i < 5; i++) {
            long id = 9200 + i;
            Map<String, Object> a = new java.util.LinkedHashMap<>(NotificationFixtures.alarm(id, "MAJOR", "ACTIVE"));
            a.put("source", Map.of("type", "RULE", "ruleId", 100 + i));
            fx.alarms.put(id, a);
            notifications.handle(alarmRequest(id, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                    List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
        }
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(3);
        clock.advanceBy(Duration.ofSeconds(61));
        aggregator.flushDue();
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(4);
        assertThat(deliveries("status = 'DIGESTED'")).isEqualTo(2);
    }

    @Test
    @DisplayName("[RUL-02.07][AT-RUL-09.3][TC-RUL-061] 매주 일요일 반복 무음(공간 31) → 일요일 알람은 알림 0건(SKIPPED·SILENCED), 월요일 00:00 이후 새 알람은 발송")
    void recurringSilence() {
        fx.silences.add(Map.of("silenceId", "1", "kind", "RECURRING", "target", Map.of("type", "SPACE", "id", "31"),
                "recurrence", Map.of("daysOfWeek", List.of(7), "from", "00:00", "to", "24:00", "timezone", "Asia/Seoul")));
        clock.set(Instant.parse("2026-03-08T05:00:00Z"));   // 일 14:00
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
        assertThat(deliveries("status = 'SKIPPED' AND skip_reason = 'SILENCED'")).isEqualTo(1);
        assertThat(TELEGRAM.calls).isEmpty();

        clock.set(Instant.parse("2026-03-08T15:00:00Z"));   // 월 00:00
        fx.alarms.put(9002L, NotificationFixtures.alarm(9002, "MAJOR", "ACTIVE"));
        notifications.handle(alarmRequest(9002, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);
    }
}
