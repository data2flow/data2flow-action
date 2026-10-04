package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.test.notification.ChannelTestPeer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 알림 채널 SPI(OPS-06.06, BR-OPS-32): 가짜 채널을 등록하면 공통 계층 코드를 바꾸지 않고 묶음·재시도·링크 대체가 동작한다 */
class NotificationChannelSpiIT extends NotificationITSupport {

    @Test
    @DisplayName("[OPS-06.06][AT-OPS-12.5][TC-OPS-141] 가짜 채널(buttons=false): 같은 알람 6건·첫 2회 실패 → 요약 1건, 3번째 시도 성공, 본문에 [확인] 대신 바로가기 링크")
    void fakeChannel() {
        fx.channels.put(NotificationFixtures.FAKE_CHANNEL, NotificationFixtures.fakeChannel(NotificationFixtures.FAKE_CHANNEL));
        fx.users.put(5L, Map.of("userId", "5", "role", "OPERATOR", "active", true, "links", Map.of("FAKE", "ops"),
                "spaceScope", Map.of("unrestricted", true)));
        coreCaches().invalidateAll();
        int before = fake.sent().size();
        for (int i = 0; i < 6; i++) {
            long id = 9300 + i;
            fx.alarms.put(id, NotificationFixtures.alarm(id, "MAJOR", "ACTIVE"));
            notifications.handle(alarmRequest(id, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, 60,
                    List.of(NotificationRecipient.user(5, "FAKE")), null));
        }
        fake.nextReply(ChannelTestPeer.Reply.TRANSIENT);
        fake.nextReply(ChannelTestPeer.Reply.TRANSIENT);
        clock.advanceBy(Duration.ofSeconds(60));
        aggregator.flushDue();
        advanceToNextRetryAndRun();
        advanceToNextRetryAndRun();

        assertThat(fake.sent().size() - before).isEqualTo(1);
        ChannelMessage m = fake.sent().get(fake.sent().size() - 1);
        assertThat(m.body()).contains("같은 알람 6건").contains("https://data2flow.java21.net/alarms/93");
        assertThat(m.buttons()).isEmpty();
        assertThat(deliveries("status = 'SENT' AND channel_type = 'FAKE' AND attempt = 3 AND digest_count = 6")).isEqualTo(1);
        assertThat(deliveries("status = 'DIGESTED'")).isEqualTo(6);
    }
}
