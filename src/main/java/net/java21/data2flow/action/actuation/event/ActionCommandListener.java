package net.java21.data2flow.action.actuation.event;

import com.rabbitmq.client.Channel;
import net.java21.data2flow.action.actuation.service.ActionRequestHandler;
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
 * {@code action.commands} 소비(Quorum, prefetch 20, delivery-limit 5, DLX → {@code action.commands.dlq}, ADR-020).
 * DB 커밋이 끝난 뒤 ACK하고, 일시 오류는 NACK(재전달 → 한도를 넘으면 DLQ), 형식 오류·모르는 버전은 재시도 없이 DLQ(reject).
 */
public class ActionCommandListener implements ChannelAwareMessageListener {

    private static final Logger log = LoggerFactory.getLogger(ActionCommandListener.class);

    private final ActionRequestHandler handler;
    private final MessageCodec codec = MessageCodec.create();

    public ActionCommandListener(ActionRequestHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        ActionRequest req;
        try {
            req = codec.read(message.getBody(), ActionRequest.class);
        } catch (MessageFormatException e) {
            log.warn("행동 요청을 읽을 수 없어 DLQ로 보냅니다: {}", e.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        Object requestId = message.getMessageProperties().getHeaders().get("X-REQUEST-ID");
        if (requestId != null) {
            MDC.put("requestId", requestId.toString());
        }
        try {
            handler.handle(req);
            channel.basicAck(tag, false);
        } catch (MessageFormatException e) {
            log.warn("처리할 수 없는 행동 요청을 DLQ로 보냅니다 messageId={}: {}", req.messageId(), e.getMessage());
            channel.basicReject(tag, false);
        } catch (RuntimeException e) {
            log.warn("행동 요청 처리 실패(다시 받음) messageId={}: {}", req.messageId(), e.toString());
            channel.basicNack(tag, false, true);
        } finally {
            MDC.remove("requestId");
        }
    }
}
