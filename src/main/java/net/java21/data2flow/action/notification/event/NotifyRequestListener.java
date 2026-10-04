package net.java21.data2flow.action.notification.event;

import com.rabbitmq.client.Channel;
import net.java21.data2flow.action.notification.service.NotificationService;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;

/**
 * {@code action.notifications} 소비(EVT-RUL-03, Quorum, delivery-limit 5, DLX → {@code action.notifications.dlq}, ADR-020).
 * 발송 행을 커밋한 뒤 ACK한다. 일시 오류(core 응답 없음 등)는 NACK(다시 받음), 형식 오류는 재시도 없이 DLQ.
 */
public class NotifyRequestListener implements ChannelAwareMessageListener {

    private static final Logger log = LoggerFactory.getLogger(NotifyRequestListener.class);
    private final NotificationService service;
    private final MessageCodec codec = MessageCodec.create();

    public NotifyRequestListener(NotificationService service) {
        this.service = service;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        ActionRequest req;
        try {
            req = codec.read(message.getBody(), ActionRequest.class);
        } catch (MessageFormatException e) {
            log.warn("알림 요청을 읽을 수 없어 DLQ로 보냅니다: {}", e.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        Object requestId = message.getMessageProperties().getHeaders().get("X-REQUEST-ID");
        if (requestId != null) {
            MDC.put("requestId", requestId.toString());
        }
        try {
            service.handle(req);
            channel.basicAck(tag, false);
        } catch (MessageFormatException e) {
            log.warn("처리할 수 없는 알림 요청을 DLQ로 보냅니다 messageId={}: {}", req.messageId(), e.getMessage());
            channel.basicReject(tag, false);
        } catch (RuntimeException e) {
            log.warn("알림 요청 처리 실패(다시 받음) messageId={}: {}", req.messageId(), e.toString());
            channel.basicNack(tag, false, true);
        } finally {
            MDC.remove("requestId");
        }
    }
}
