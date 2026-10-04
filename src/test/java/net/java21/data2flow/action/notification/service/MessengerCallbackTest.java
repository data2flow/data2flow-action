package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationErrorCode;
import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.action.notification.channel.ChannelRegistry;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.notification.service.NotificationCoreClient.LinkedUser;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.notification.CallbackAction;
import net.java21.data2flow.contracts.notification.ChannelButton;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import net.java21.data2flow.contracts.test.notification.FakeNotificationChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessengerCallbackTest {

    private final FakeNotificationChannel channel = new FakeNotificationChannel(true, "fake-secret");
    private final NotificationCoreClient core = mock(NotificationCoreClient.class);
    private final DeliveryRepository deliveries = mock(DeliveryRepository.class);
    private final MessengerCallbackService service = new MessengerCallbackService(new ChannelRegistry(List.of(channel)), core, deliveries);
    private final UUID deliveryId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        when(core.channels(eq(null), eq("FAKE"))).thenReturn(List.of(ChannelDefinition.from(Json.MAPPER.valueToTree(
                NotificationFixtures.fakeChannel(6)))));
        MessagePayload payload = new MessagePayload("ops", "실습실 고온", "실습실 온도 27.6℃", "https://data2flow.java21.net/alarms/9001",
                List.of(new ChannelButton("확인", CallbackAction.ACK, 9001L, deliveryId.toString())), "ko", AlarmSeverity.MAJOR, null);
        when(deliveries.findForCallback(deliveryId)).thenReturn(Optional.of(new Delivery(deliveryId, 1, "k", 6, "FAKE", "ALARM", "9001",
                9001L, null, "USER:5", 5L, null, "alarm.raised", "MAJOR", null, payload, 1, DeliveryStatus.SENT, null, 1, null, null,
                "fake-1", Instant.parse("2026-03-02T00:00:00Z"), Instant.parse("2026-03-02T00:00:00Z"))));
    }

    @Test
    @DisplayName("[RUL-05.02][AT-RUL-10.1][TC-RUL-093] 연결된 OPERATOR가 [확인] → 그 사용자 권한으로 알람 ACKNOWLEDGED, 같은 메시지를 결과로 갱신(버튼 제거)")
    void ackUpdatesMessage() {
        when(core.linkedUser("FAKE", "777")).thenReturn(Optional.of(new LinkedUser(5, 1)));
        when(core.alarmAction(eq(9001L), eq("ack"), any(), eq(null))).thenReturn(200);

        MessengerCallbackService.Result r = service.handle("fake", channel.callback("ACK|9001|" + deliveryId, "777"));

        assertThat(r.outcome()).isEqualTo("ACKED");
        verify(core).alarmAction(9001L, "ack", new LinkedUser(5, 1), null);
        assertThat(channel.sent()).hasSize(1);
        assertThat(channel.sent().get(0).replaceExternalMessageId()).isEqualTo("fake-1");
        assertThat(channel.sent().get(0).buttons()).isEmpty();
        assertThat(channel.sent().get(0).body()).contains("확인됨");
        verify(deliveries).updatePayload(eq(deliveryId), any());
    }

    @Test
    @DisplayName("[RUL-05.02][AT-RUL-10.2][TC-RUL-094] 연결 안 된 사용자가 [확인] → 처리하지 않고(core 호출 없음) 연결 안내만 보낸다")
    void notLinked() {
        when(core.linkedUser("FAKE", "888")).thenReturn(Optional.empty());

        MessengerCallbackService.Result r = service.handle("FAKE", channel.callback("ACK|9001|" + deliveryId, "888"));

        assertThat(r.outcome()).isEqualTo("NOT_LINKED");
        verify(core, never()).alarmAction(anyLong(), anyString(), any(), any());
        assertThat(channel.sent()).singleElement().satisfies(m -> {
            assertThat(m.recipientAddress()).isEqualTo("888");
            assertThat(m.body()).contains("연결되지 않았습니다");
        });
    }

    @Test
    @DisplayName("[RUL-05.02][BR-RUL-18] [30분 무음] → core 무음(30분), 권한 없음(403)이면 메시지를 바꾸지 않는다")
    void muteAndForbidden() {
        when(core.linkedUser("FAKE", "777")).thenReturn(Optional.of(new LinkedUser(5, 1)));
        when(core.alarmAction(eq(9001L), eq("mute"), any(), eq(30))).thenReturn(200);
        assertThat(service.handle("FAKE", channel.callback("MUTE_30M|9001|" + deliveryId, "777")).outcome()).isEqualTo("MUTED");
        when(core.alarmAction(eq(9001L), eq("ack"), any(), eq(null))).thenReturn(403);
        assertThat(service.handle("FAKE", channel.callback("ACK|9001|" + deliveryId, "777")).outcome()).isEqualTo("FORBIDDEN");
        assertThat(channel.sent()).hasSize(1);
    }

    @Test
    @DisplayName("[RUL-05.02][API-RUL-30] 연결 코드(/start) → 계정 연결 확정 후 안내, 틀린 코드는 실패 안내. 시크릿이 틀린 콜백은 401")
    void linkAndVerify() {
        when(core.confirmLink("FAKE", "ABC123", "777")).thenReturn(Optional.of(new LinkedUser(5, 1)));
        when(core.confirmLink("FAKE", "WRONG", "777")).thenReturn(Optional.empty());
        assertThat(service.handle("FAKE", channel.linkStart("ABC123", "777")).outcome()).isEqualTo("LINKED");
        assertThat(service.handle("FAKE", channel.linkStart("WRONG", "777")).outcome()).isEqualTo("LINK_FAILED");
        assertThat(service.handle("FAKE", channel.callback("DANCE|1|2", "777")).handled()).isFalse();
        assertThat(service.handle("FAKE", channel.callback("APPROVE|1|2", "777")).outcome()).isEqualTo("UNSUPPORTED");
        assertThatThrownBy(() -> service.handle("FAKE", channel.callbackWithWrongSecret("ACK|9001|d", "777")))
                .isInstanceOf(BusinessException.class)
                .satisfies(e -> assertThat(((BusinessException) e).getErrorCode()).isEqualTo(NotificationErrorCode.CALLBACK_REJECTED));
        assertThatThrownBy(() -> service.handle("SLACK", channel.callback("ACK|9001|d", "777"))).isInstanceOf(BusinessException.class);
        verify(core, never()).alarmAction(anyLong(), anyString(), any(), anyInt());
        assertThat(Map.of()).isEmpty();
    }
}
