package net.java21.data2flow.action.notification.channel;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.channel.telegram.TelegramChannel;
import net.java21.data2flow.action.support.FakeTelegram;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.ChannelSettings;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import net.java21.data2flow.contracts.notification.SendResult;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.test.notification.AbstractNotificationChannelContractTest;
import net.java21.data2flow.contracts.test.notification.ChannelTestPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 텔레그램 채널 계약 시험(OPS-06.06 TC-OPS-142): 공용 키트를 Bot API 대역(MockWebServer)으로 돌린다. 실제 api.telegram.org는 부르지 않는다.
 */
class TelegramChannelContractTest extends AbstractNotificationChannelContractTest {

    private static final FakeTelegram PEER = new FakeTelegram();
    private final TelegramChannel channel = new TelegramChannel(new NotificationProperties.Telegram(true, PEER.url(), null));

    @AfterAll
    static void stop() throws java.io.IOException {
        PEER.server.shutdown();
    }

    @BeforeEach
    void reset() {
        PEER.reset();
    }

    @Override
    protected NotificationChannel channel() {
        return channel;
    }

    @Override
    protected ChannelSettings settings() {
        return new ChannelSettings(4, 1, Json.MAPPER.valueToTree(Map.of("chatIds", List.of("-100777"), "botUsername", "data2flow_test_bot")),
                Map.of(TelegramChannel.BOT_TOKEN, Secret.of(FakeTelegram.TOKEN), TelegramChannel.WEBHOOK_SECRET, Secret.of(FakeTelegram.SECRET)));
    }

    @Override
    protected ChannelTestPeer peer() {
        return PEER;
    }

    @Override
    protected String recipientAddress() {
        return "-100777";
    }

    @Test
    @DisplayName("[OPS-06.01][TC-OPS-142] sendMessage: MarkdownV2 이스케이프·인라인 버튼(callback_data = ACK|9001|d-1), 토큰은 오류 문구에 없음")
    void sendMessageShape() {
        channel.send(settings(), message().adaptTo(channel.capabilities()));
        FakeTelegram.Call call = PEER.calls("sendMessage").get(0);
        assertThat(call.token()).isEqualTo(FakeTelegram.TOKEN);
        assertThat(call.body().path("parse_mode").asString()).isEqualTo("MarkdownV2");
        assertThat(call.body().path("text").asString()).contains("27\\.6℃").contains("https://data2flow\\.java21\\.net/alarms/9001");
        assertThat(call.body().path("reply_markup").path("inline_keyboard").get(0).get(0).path("callback_data").asString())
                .isEqualTo("ACK|9001|d-1");
        SendResult wrongToken = channel.send(new ChannelSettings(4, 1, settings().config(), Map.of(TelegramChannel.BOT_TOKEN, Secret.of("bad"))),
                message());
        assertThat(wrongToken.outcome()).isEqualTo(SendResult.Outcome.PERMANENT_FAILURE);
        assertThat(wrongToken.error()).contains("401").doesNotContain("bad");
    }

    @Test
    @DisplayName("[RUL-03.05][TC-RUL-080] 429 retry_after 7 → 일시 실패 + 재시도 대기 7초, 500 → 일시 실패, 버튼 응답 뒤 갱신은 editMessageText")
    void retryAfterAndEdit() {
        PEER.nextStatus(429, 7);
        SendResult r = channel.send(settings(), message());
        assertThat(r.outcome()).isEqualTo(SendResult.Outcome.TRANSIENT_FAILURE);
        assertThat(r.retryAfter()).isEqualTo(Duration.ofSeconds(7));
        PEER.nextStatus(500, null);
        assertThat(channel.send(settings(), message()).retryable()).isTrue();
        ChannelMessage edit = new ChannelMessage("k-edit", "-100777", null, "확인됨", null, null, List.of(), "ko", "101");
        assertThat(channel.send(settings(), edit).outcome()).isEqualTo(SendResult.Outcome.SUCCESS);
        assertThat(PEER.calls("editMessageText").get(0).body().path("message_id").asLong()).isEqualTo(101);
    }

    @Test
    @DisplayName("[OPS-06.01] 어댑터가 꺼져 있으면(기본) 쓸 수 없음 + 영구 실패 CHANNEL_UNAVAILABLE, 상대를 부르지 않는다. 토큰이 없으면 영구 실패")
    void disabledByDefault() {
        TelegramChannel off = new TelegramChannel(NotificationProperties.defaults().telegram());
        assertThat(off.available()).isFalse();
        assertThat(NotificationProperties.defaults().telegram().apiBaseUrl()).isEqualTo("https://api.telegram.org");
        assertThat(off.send(settings(), message()).error()).startsWith("CHANNEL_UNAVAILABLE");
        assertThat(off.registerWebhook(settings(), "https://x")).isEqualTo("CHANNEL_UNAVAILABLE");
        assertThat(channel.send(new ChannelSettings(4, 1, settings().config(), Map.of()), message()).error()).startsWith("BOT_TOKEN_MISSING");
        assertThat(PEER.calls).isEmpty();
    }

    @Test
    @DisplayName("[OPS-06.01] 저장 때 setWebhook(콜백 주소 + 시크릿 토큰), 버튼 응답은 answerCallbackQuery")
    void webhookAndAnswer() {
        assertThat(channel.registerWebhook(settings(), "https://data2flow.java21.net/hooks/messenger/telegram")).isNull();
        FakeTelegram.Call call = PEER.calls("setWebhook").get(0);
        assertThat(call.body().path("secret_token").asString()).isEqualTo(FakeTelegram.SECRET);
        assertThat(call.body().path("url").asString()).endsWith("/hooks/messenger/telegram");
        channel.acknowledge(settings(), channel.handleCallback(settings(), PEER.callback("ACK|9001|d-1", "777")).orElseThrow(), "확인됨");
        assertThat(PEER.calls("answerCallbackQuery").get(0).body().path("callback_query_id").asString()).isEqualTo("cq-1");
    }
}
