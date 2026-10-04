package net.java21.data2flow.action.sink.event;

import net.java21.data2flow.action.sink.service.SinkConnectionCache;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * 설정 변경 수신({@code data2flow.config} fanout, 인스턴스별 임시 큐 {@code action.sink-config.*}). SINK_CONNECTION이면 그 연결 정의와 풀을,
 * SETTING·UNKNOWN이면 모두 지운다. 재연결 때도 모두 지운다(놓친 변경).
 */
public class SinkConfigListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(SinkConfigListener.class);

    private final SinkConnectionCache cache;
    private final MessageCodec codec = MessageCodec.create();

    public SinkConfigListener(SinkConnectionCache cache) {
        this.cache = cache;
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
            case SINK_CONNECTION -> {
                try {
                    cache.invalidate(Long.parseLong(change.id()));
                } catch (NumberFormatException e) {
                    cache.invalidateAll();
                }
            }
            case SETTING, UNKNOWN -> cache.invalidateAll();
            default -> {
                // 다른 패키지·서비스용
            }
        }
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }
}
