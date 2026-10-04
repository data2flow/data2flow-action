package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.alarm.AlarmSnapshot;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.AlarmStateChanged;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 에스컬레이션(RUL-03.03, BR-RUL-16): 1단계 10분·2단계 10분·3단계 */
class EscalationIT extends NotificationITSupport {

    @BeforeEach
    void policy() {
        fx.policies.put(3L, Map.of("notificationPolicyId", "3", "organizationId", "1", "minSeverity", "MINOR",
                "recipients", List.of(Map.of("type", "USER", "id", "5")), "channels", List.of("TELEGRAM"), "renotifyMinutes", 30,
                "aggregateWindowSec", 0, "notifyOnClear", false,
                "steps", List.of(Map.of("stepNo", 1, "waitMinutes", 10, "recipients", List.of(Map.of("type", "USER", "id", "5"))),
                        Map.of("stepNo", 2, "waitMinutes", 10, "recipients", List.of(Map.of("type", "USER", "id", "6"))),
                        Map.of("stepNo", 3, "waitMinutes", 10, "recipients", List.of(Map.of("type", "USER", "id", "7"))))));
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), 3L));
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);
    }

    private List<String> chats() {
        return TELEGRAM.calls("sendMessage").stream().map(c -> c.body().path("chat_id").asString()).toList();
    }

    @Test
    @DisplayName("[RUL-03.03][AT-RUL-12.1][TC-RUL-074][TC-RUL-077] 10분 미확인 → 9분 59초엔 0건, 10분에 2단계(6)에게 발송·이력 ESCALATED, 다시 10분 뒤 3단계(7)")
    void escalates() {
        clock.advanceBy(Duration.ofMinutes(9).plusSeconds(59));
        escalations.processDue();
        assertThat(chats()).containsExactly("555");
        clock.advanceBy(Duration.ofSeconds(1));
        escalations.processDue();
        assertThat(chats()).containsExactly("555", "666");
        assertThat(count("SELECT count(*) FROM data2flow_action.outboxes WHERE kind = 'CALLBACK' AND routing_key = '/internal/core/alarms/9001/events'"))
                .isEqualTo(1);
        assertThat(deliveries("step_no = 2 AND event = 'alarm.escalated'")).isEqualTo(1);
        clock.advanceBy(Duration.ofMinutes(10));
        escalations.processDue();
        assertThat(chats()).containsExactly("555", "666", "777");
        clock.advanceBy(Duration.ofMinutes(30));
        assertThat(escalations.processDue()).isZero();
    }

    @Test
    @DisplayName("[RUL-03.03][AT-RUL-12.2][TC-RUL-075][TC-RUL-077] 8분에 확인(alarm.acked) → 남은 단계 취소, 10분에 2단계 발송 없음")
    void ackCancels() {
        clock.advanceBy(Duration.ofMinutes(8));
        Map<String, Object> acked = NotificationFixtures.alarm(NotificationFixtures.ALARM, "MAJOR", "ACKNOWLEDGED");
        fx.alarms.put(NotificationFixtures.ALARM, acked);
        publish(EventType.ALARM_ACKED, new AlarmStateChanged(CODEC.mapper().convertValue(acked, AlarmSnapshot.class), null, clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.notification_escalations "
                + "WHERE status = 'CANCELLED'") == 1);
        clock.advanceBy(Duration.ofMinutes(2));
        escalations.processDue();
        assertThat(chats()).containsExactly("555");
    }

    @Test
    @DisplayName("[RUL-03.03][BR-RUL-16] 확인 이벤트를 놓쳐도 기한에 알람 상태를 다시 확인해 취소한다")
    void recheckAtDueTime() {
        fx.alarms.put(NotificationFixtures.ALARM, NotificationFixtures.alarm(NotificationFixtures.ALARM, "MAJOR", "CLEARED"));
        clock.advanceBy(Duration.ofMinutes(10));
        escalations.processDue();
        assertThat(chats()).containsExactly("555");
        assertThat(count("SELECT count(*) FROM data2flow_action.notification_escalations WHERE status = 'CANCELLED'")).isEqualTo(1);
    }
}
