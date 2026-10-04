package net.java21.data2flow.action.sink.event;

import net.java21.data2flow.action.sink.service.SinkConnectionCache;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.time.Clock;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class SinkConfigListenerTest {

    private final SinkConnectionCache cache = mock(SinkConnectionCache.class);
    private final SinkConfigListener listener = new SinkConfigListener(cache);

    private static Message message(ConfigChangedMessage m) {
        return new Message(MessageCodec.create().write(m), new MessageProperties());
    }

    @Test
    @DisplayName("[FLW-04.01] SINK_CONNECTION 변경은 그 연결 정의·풀만, SETTING은 전체를 지우고, 다른 종류·읽을 수 없는 메시지는 무시")
    void invalidates() {
        listener.onMessage(message(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SINK_CONNECTION, 4, 2, 1, Clock.systemUTC())));
        verify(cache).invalidate(4);
        listener.onMessage(message(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.SETTING, 1, 2, 1, Clock.systemUTC())));
        verify(cache).invalidateAll();
        SinkConnectionCache other = mock(SinkConnectionCache.class);
        SinkConfigListener l2 = new SinkConfigListener(other);
        l2.onMessage(message(ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.DEVICE, 4, 2, 1, Clock.systemUTC())));
        l2.onMessage(new Message("x".getBytes(), new MessageProperties()));
        verifyNoInteractions(other);
    }
}
