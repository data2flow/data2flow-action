package net.java21.data2flow.action.sink.event;

import com.rabbitmq.client.Channel;
import net.java21.data2flow.action.sink.service.SinkWriteService;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;
import java.util.Optional;

/**
 * {@code action.sinks} 소비(Quorum, delivery-limit 5, DLX → {@code action.sinks.dlq}, ADR-020). 배치 행을 DB에 커밋한 뒤 ACK하고, 그 다음
 * 대상에 써 본다(쓰기 실패는 큐가 아니라 배치 행의 재시도로 다룬다 — 대상 DB가 오래 멈춰도 큐 재전달 한도에 걸리지 않음).
 * 형식 오류는 재시도 없이 DLQ, DB 장애는 NACK(재전달).
 */
public class SinkRequestListener implements ChannelAwareMessageListener {

    private static final Logger log = LoggerFactory.getLogger(SinkRequestListener.class);

    private final SinkWriteService service;
    private final MessageCodec codec = MessageCodec.create();

    public SinkRequestListener(SinkWriteService service) {
        this.service = service;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        ActionRequest req;
        try {
            req = codec.read(message.getBody(), ActionRequest.class);
        } catch (MessageFormatException e) {
            log.warn("Sink 요청을 읽을 수 없어 DLQ로 보냅니다: {}", e.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        Object requestId = message.getMessageProperties().getHeaders().get("X-REQUEST-ID");
        if (requestId != null) {
            MDC.put("requestId", requestId.toString());
        }
        try {
            Optional<String> key = service.accept(req);
            channel.basicAck(tag, false);
            key.ifPresent(k -> {
                try {
                    service.writeNow(req.organizationId(), k);
                } catch (RuntimeException e) {
                    log.warn("Sink 바로 쓰기 실패(재시도 작업이 이어받음) key={}: {}", k, e.toString());
                }
            });
        } catch (MessageFormatException e) {
            log.warn("처리할 수 없는 Sink 요청을 DLQ로 보냅니다 messageId={}: {}", req.messageId(), e.getMessage());
            channel.basicReject(tag, false);
        } catch (RuntimeException e) {
            log.warn("Sink 요청 처리 실패(다시 받음) messageId={}: {}", req.messageId(), e.toString());
            channel.basicNack(tag, false, true);
        } finally {
            MDC.remove("requestId");
        }
    }
}
