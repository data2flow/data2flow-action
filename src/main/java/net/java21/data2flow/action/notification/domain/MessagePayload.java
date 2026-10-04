package net.java21.data2flow.action.notification.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.notification.ChannelButton;
import net.java21.data2flow.contracts.notification.ChannelMessage;

import java.util.List;

/**
 * 보낼 공통 메시지(notification_deliveries.payload). 채널 형식이 아니라 공통 모양으로 저장하고, 보낼 때마다 채널 기능에 맞춘다
 * ({@link ChannelMessage#adaptTo}, BR-OPS-32). 비밀값은 없다.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record MessagePayload(String address, String title, String body, String link, List<ChannelButton> buttons, String locale,
                             AlarmSeverity severity, String replaceExternalMessageId) {

    public MessagePayload {
        buttons = buttons == null ? List.of() : List.copyOf(buttons);
    }

    public ChannelMessage toMessage(String idempotencyKey) {
        return new ChannelMessage(idempotencyKey, address, title, body, severity, link, buttons, locale, replaceExternalMessageId);
    }

    public MessagePayload withBody(String newBody, List<ChannelButton> newButtons, String replaceId) {
        return new MessagePayload(address, title, newBody, link, newButtons, locale, severity, replaceId);
    }
}
