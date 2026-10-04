package net.java21.data2flow.action.notification.channel;

import net.java21.data2flow.contracts.notification.ChannelSettings;

/**
 * 선택 기능: 채널 저장 때 상대 서비스에 콜백 주소를 등록한다(텔레그램 {@code setWebhook}, OPS-06.01). SPI({@code NotificationChannel})에
 * 없는 기능이라 action 안의 확장 인터페이스로 두고, 공통 계층은 채널 키가 아니라 "이 기능을 지원하는가"로만 판단한다(BR-OPS-32).
 */
public interface WebhookRegistrar {

    /**
     * 콜백 주소와 시크릿을 등록한다.
     *
     * @return 실패 원인. 성공이면 null
     */
    String registerWebhook(ChannelSettings settings, String url);
}
