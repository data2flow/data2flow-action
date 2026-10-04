package net.java21.data2flow.action.notification.event;

import com.rabbitmq.client.Channel;
import net.java21.data2flow.action.notification.service.EscalationService;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.event.EventPayload;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.message.event.AlarmStateChanged;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;
import java.util.List;

/**
 * {@code action.notification.events} 소비: {@code alarm.acked}·{@code alarm.cleared}(EVT-RUL-02) → 남은 에스컬레이션 취소(BR-RUL-16).
 * 취소는 멱등이다.
 */
public class AlarmEventListener implements ChannelAwareMessageListener {

    public static final List<String> ROUTING_KEYS = List.of("alarm.acked", "alarm.cleared");
    private static final Logger log = LoggerFactory.getLogger(AlarmEventListener.class);
    private final EscalationService escalations;
    private final MessageCodec codec = MessageCodec.create();

    public AlarmEventListener(EscalationService escalations) {
        this.escalations = escalations;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        DomainEvent<? extends EventPayload> event;
        try {
            event = codec.readEvent(message.getBody());
        } catch (MessageFormatException e) {
            log.warn("알람 이벤트를 읽을 수 없어 DLQ로 보냅니다: {}", e.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        try {
            handle(event);
            channel.basicAck(tag, false);
        } catch (RuntimeException e) {
            log.warn("알람 이벤트 처리 실패(다시 받음) type={}: {}", event.type(), e.toString());
            channel.basicNack(tag, false, true);
        }
    }

    public void handle(DomainEvent<? extends EventPayload> event) {
        if (event.payload() instanceof AlarmStateChanged changed && changed.alarm() != null) {
            escalations.cancel(event.organizationId(), changed.alarm().id());
        }
    }
}
