package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.action.support.FakeTelegram;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 알림 발송 전 구간(큐 → 공통 계층 → 텔레그램 대역): RUL-03.01·03.04·03.05·05.02, OPS-06.01·06.05 */
class NotificationDeliveryIT extends NotificationITSupport {

    @Test
    @DisplayName("[RUL-03.01][AT-RUL-07.1][TC-RUL-065] MAJOR 알람 → 웹 알림(권한 있는 사용자) 1건 + 텔레그램 1건, 이력 2건 SENT, notification.delivered 2건")
    void webAndTelegram() {
        send(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM"), NotificationRecipient.user(5, NotificationRecipient.WEB)), null));

        await().atMost(Duration.ofSeconds(20)).until(() -> deliveries("status = 'SENT'") == 2);
        assertThat(TELEGRAM.calls("sendMessage")).singleElement()
                .satisfies(c -> assertThat(c.body().path("chat_id").asString()).isEqualTo("555"));
        assertThat(outbox("notification.delivered")).isEqualTo(2);
        assertThat(deliveries("channel_type = 'WEB' AND channel_id = 0 AND recipient_key = 'USER:5'")).isEqualTo(1);
    }

    @Test
    @DisplayName("[RUL-03.04][AT-RUL-07.3][TC-RUL-078] 템플릿 {{device.name}} → 실제 기기 이름, 링크 https://data2flow.java21.net/alarms/{id}, [확인]·[30분 무음] 버튼")
    void template() {
        fx.template = Map.of("subject", "[{{severity}}] {{title}}", "body", "{{device.name}} {{value}}℃ {{link}}");
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));

        String body = jdbc.sql("SELECT payload->>'body' FROM data2flow_action.notification_deliveries").query(String.class).single();
        assertThat(body).isEqualTo("실습실 온도계 27.6℃ https://data2flow.java21.net/alarms/9001");
        var keyboard = TELEGRAM.calls("sendMessage").get(0).body().path("reply_markup").path("inline_keyboard").get(0);
        assertThat(keyboard.get(0).path("callback_data").asString()).startsWith("ACK|9001|");
        assertThat(keyboard.get(1).path("callback_data").asString()).startsWith("MUTE_30M|9001|");
    }

    @Test
    @DisplayName("[RUL-03.05][TC-RUL-081][TC-OPS-066] 같은 NotificationRequest(멱등 키)가 큐에 3번(같은 messageId 2번 + 새 messageId) → 텔레그램 1회, executed_actions 1행, 이력 1건")
    void idempotent() {
        ActionRequest req = alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null);
        send(req);
        send(req);
        send(ActionRequest.notify(req.organizationId(), req.idempotencyKey(), req.source(), null, req.notificationRequest(), clock));

        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.processed_messages "
                + "WHERE consumer = 'action.notifications'") == 2);
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);
        assertThat(count("SELECT count(*) FROM data2flow_action.executed_actions WHERE kind = 'NOTIFY'")).isEqualTo(1);
        assertThat(deliveries("true")).isEqualTo(1);
        // 재처리(서비스 재시작 뒤 같은 요청) → 실제 발송은 여전히 1회
        notifications.handle(req);
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);
    }

    @Test
    @DisplayName("[OPS-06.05][AT-OPS-13.1][TC-OPS-068] 최소 심각도 MAJOR인 사용자에게 MINOR 알람 → 미발송(SKIPPED·SEVERITY)")
    void minSeverity() {
        Map<String, Object> u = new LinkedHashMap<>(NotificationFixtures.user(6, "OPERATOR", "666"));
        u.put("minSeverity", "MAJOR");
        fx.users.put(6L, u);
        fx.alarms.put(9002L, NotificationFixtures.alarm(9002, "MINOR", "ACTIVE"));
        notifications.handle(alarmRequest(9002, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MINOR, null,
                List.of(NotificationRecipient.user(6, "TELEGRAM")), null));

        assertThat(deliveries("status = 'SKIPPED' AND skip_reason = 'SEVERITY'")).isEqualTo(1);
        assertThat(TELEGRAM.calls).isEmpty();
    }

    @Test
    @DisplayName("[OPS-06.05][AT-OPS-13.2][TC-OPS-070] 방해 금지 22~07시·CRITICAL 예외 켬: 23시 CRITICAL → 발송, 23시 MAJOR → 07시까지 기다렸다가 발송")
    void doNotDisturb() {
        Map<String, Object> u = new LinkedHashMap<>(NotificationFixtures.user(5, "OPERATOR", "555"));
        u.put("dnd", Map.of("from", "22:00", "to", "07:00", "allowCritical", true));
        fx.users.put(5L, u);
        clock.set(Instant.parse("2026-03-02T14:00:00Z"));   // 서울 23:00
        fx.alarms.put(9003L, NotificationFixtures.alarm(9003, "CRITICAL", "ACTIVE"));
        notifications.handle(alarmRequest(9003, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.CRITICAL, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);

        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(1);
        clock.set(Instant.parse("2026-03-02T22:00:00Z"));   // 서울 07:00
        aggregator.flushDue();
        assertThat(TELEGRAM.calls("sendMessage")).hasSize(2);
    }

    @Test
    @DisplayName("[OPS-06.01][AT-OPS-11.1][TC-OPS-055] 테스트 발송 → 기본 채팅방에 테스트 메시지, 결과 ok·지연 시간. 잘못된 토큰은 502 CHANNEL_TEST_FAILED. 유형 목록·웹훅 등록·딥링크")
    void channelAdmin() {
        Result ok = post(api(7, 1), "/internal/action/notifications/channels/test", Map.of("channelId", "4"), null);
        assertThat(ok.status()).isEqualTo(200);
        assertThat(ok.response().path("ok").asBoolean()).isTrue();
        assertThat(TELEGRAM.calls("sendMessage").get(0).body().path("chat_id").asString()).isEqualTo(NotificationFixtures.CHAT);

        Result bad = post(api(7, 1), "/internal/action/notifications/channels/test",
                Map.of("type", "TELEGRAM", "config", Map.of("chatIds", List.of("1")), "secrets", Map.of("botToken", "wrong")), null);
        assertThat(bad.status()).isEqualTo(502);
        assertThat(bad.code()).isEqualTo("CHANNEL_TEST_FAILED");

        Result types = get(api(7, 1), "/internal/action/notifications/channel-types");
        assertThat(types.response().findValuesAsString("key")).contains("TELEGRAM", "FAKE");

        assertThat(post(api(7, 1), "/internal/action/notifications/channels/4/webhook", Map.of(), null).status()).isEqualTo(200);
        assertThat(TELEGRAM.calls("setWebhook")).hasSize(1);
        assertThat(post(api(7, 1), "/internal/action/notifications/channels/99/webhook", Map.of(), null).code()).isEqualTo("CHANNEL_NOT_FOUND");

        Result link = post(api(5, 1), "/internal/action/notifications/links", Map.of("channel", "telegram", "code", "ABC123",
                "expiresAt", "2026-03-02T00:10:00Z"), null);
        assertThat(link.response().path("deepLink").asString()).isEqualTo("https://t.me/data2flow_test_bot?start=ABC123");
    }

    @Test
    @DisplayName("[RUL-05.02][AT-RUL-10.1][TC-RUL-093] 텔레그램 [확인] 콜백(시크릿 검증) → 연결된 사용자로 core 확인, 같은 메시지 갱신. 시크릿이 틀리면 401")
    void messengerAck() {
        fx.links.put("555", 5L);
        notifications.handle(alarmRequest(NotificationFixtures.ALARM, NotificationEvents.ALARM_RAISED, 1, AlarmSeverity.MAJOR, null,
                List.of(NotificationRecipient.user(5, "TELEGRAM")), null));
        String deliveryId = jdbc.sql("SELECT id::text FROM data2flow_action.notification_deliveries").query(String.class).single();
        RestClient bff = RestClient.builder().baseUrl("http://localhost:" + port).build();

        Result r = exchange(bff.post().uri("/internal/action/notifications/callbacks/telegram").contentType(MediaType.APPLICATION_JSON)
                .header("X-Telegram-Bot-Api-Secret-Token", FakeTelegram.SECRET)
                .body(FakeTelegram.callbackJson("ACK|9001|" + deliveryId, "555", 101)));
        assertThat(r.status()).isEqualTo(200);
        assertThat(r.response().path("outcome").asString()).isEqualTo("ACKED");
        assertThat(fx.alarmActions).containsExactly("ack:9001:user=5");
        assertThat(TELEGRAM.calls("editMessageText")).hasSize(1);
        assertThat(TELEGRAM.calls("answerCallbackQuery")).hasSize(1);

        Result denied = exchange(bff.post().uri("/internal/action/notifications/callbacks/telegram").contentType(MediaType.APPLICATION_JSON)
                .header("X-Telegram-Bot-Api-Secret-Token", "nope").body(FakeTelegram.callbackJson("ACK|9001|x", "555", 101)));
        assertThat(denied.status()).isEqualTo(401);
    }

    @Test
    @DisplayName("[RUL-03.05][OPS-06.03][API-RUL-27·API-OPS-33·97] 시스템 알림 직접 발송 → 발송 이력 커서 목록, 실패 건 다시 보내기, 성공 건은 409")
    void systemNotifyHistoryAndResend() {
        TELEGRAM.nextStatus(400, null);
        Result accepted = post(api(7, 1), "/internal/action/notifications", Map.of("idempotencyKey", "backup-failed-1", "channelIds", List.of("4"),
                "severity", "MAJOR", "subject", "백업 실패", "body", "02:00 백업이 실패했습니다", "sourceType", "SYSTEM"), null);
        assertThat(accepted.status()).isEqualTo(202);
        assertThat(deliveries("status = 'FAILED'")).isEqualTo(1);

        Result page = get(api(7, 1), "/internal/action/notifications/deliveries?status=failed&size=1");
        assertThat(page.body().path("responses")).hasSize(1);
        String id = page.body().path("responses").get(0).path("deliveryId").asString();
        Result resent = post(api(7, 1), "/internal/action/notifications/deliveries/" + id + "/resend", Map.of(), null);
        assertThat(resent.status()).isEqualTo(200);
        assertThat(deliveries("status = 'SENT'")).isEqualTo(1);
        String sentId = jdbc.sql("SELECT id::text FROM data2flow_action.notification_deliveries WHERE status = 'SENT'").query(String.class).single();
        assertThat(post(api(7, 1), "/internal/action/notifications/deliveries/" + sentId + "/resend", Map.of(), null).status()).isEqualTo(409);
        assertThat(get(api(7, 1), "/internal/action/notifications/deliveries?cursor=%%%").status()).isEqualTo(400);
    }
}
