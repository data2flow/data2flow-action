package net.java21.data2flow.action.notification.channel;

import net.java21.data2flow.contracts.notification.CallbackCommand;
import net.java21.data2flow.contracts.notification.ChannelSettings;

/**
 * 선택 기능: 버튼 응답을 받았다고 상대에 알린다(텔레그램 {@code answerCallbackQuery}: 응답하지 않으면 버튼이 계속 도는 표시로 남는다).
 * 공통 계층은 이 기능을 지원하는 채널에만 부른다.
 */
public interface CallbackAcknowledger {

    void acknowledge(ChannelSettings settings, CallbackCommand command, String text);
}
