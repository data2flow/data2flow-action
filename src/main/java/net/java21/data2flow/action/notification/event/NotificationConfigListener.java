package net.java21.data2flow.action.notification.event;

import net.java21.data2flow.action.notification.service.NotificationCoreClient;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * 설정 변경(fanout {@code data2flow.config}, 인스턴스별 임시 큐): 알림 정의가 바뀌면 캐시를 지운다(ADR-048 종류
 * NOTIFICATION_CHANNEL·NOTIFICATION_POLICY·NOTIFICATION_TEMPLATE·SILENCE·ON_CALL·NOTIFY_PREFERENCE, 모르는 종류 UNKNOWN·SETTING).
 */
public class NotificationConfigListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationConfigListener.class);
    private final NotificationCoreClient core;
    private final MessageCodec codec = MessageCodec.create();

    public NotificationConfigListener(NotificationCoreClient core) {
        this.core = core;
    }

    @Override
    public void onMessage(Message message) {
        try {
            apply(codec.read(message.getBody(), ConfigChangedMessage.class));
        } catch (RuntimeException e) {
            log.warn("읽을 수 없는 설정 변경 메시지를 무시합니다: {}", e.getMessage());
        }
    }

    public void apply(ConfigChangedMessage change) {
        switch (change.entityType()) {
            case NOTIFICATION_CHANNEL, NOTIFICATION_POLICY, NOTIFICATION_TEMPLATE, SILENCE, ON_CALL, NOTIFY_PREFERENCE, SETTING, UNKNOWN ->
                    core.invalidateAll();
            default -> {
                // 다른 패키지·서비스용
            }
        }
    }
}
