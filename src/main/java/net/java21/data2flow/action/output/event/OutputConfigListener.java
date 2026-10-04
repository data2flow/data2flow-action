package net.java21.data2flow.action.output.event;

import net.java21.data2flow.action.output.service.DeviceContextCache;
import net.java21.data2flow.action.output.service.OutputConnectionRegistry;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * 설정 변경 수신({@code data2flow.config} fanout, 임시 큐 {@code action.output-config.*}). OUTPUT이면 연결 정의를 바로 다시 읽고,
 * DEVICE·SPACE·GROUP이면 기기 맥락 캐시를 지운다. SETTING·UNKNOWN·재연결이면 둘 다.
 */
public class OutputConfigListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(OutputConfigListener.class);

    private final OutputConnectionRegistry registry;
    private final DeviceContextCache contexts;
    private final MessageCodec codec = MessageCodec.create();

    public OutputConfigListener(OutputConnectionRegistry registry, DeviceContextCache contexts) {
        this.registry = registry;
        this.contexts = contexts;
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
            case OUTPUT -> registry.invalidate();
            case DEVICE, SPACE, GROUP -> contexts.invalidateAll();
            case SETTING, UNKNOWN -> invalidateAll();
            default -> {
                // 다른 패키지용
            }
        }
    }

    public void invalidateAll() {
        registry.invalidate();
        contexts.invalidateAll();
    }
}
