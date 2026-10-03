package net.java21.data2flow.action.outbox;

import net.java21.data2flow.action.outbox.OutboxRepository.OutboxMessage;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/** RabbitMQ 발행: publisher confirm(ack)을 받아야 성공(reliability-and-ha.md ⑦). 영속 메시지, 공통 헤더(messageId·v·schema·organizationId) */
public class RabbitOutboxDispatcher implements OutboxDispatcher {

    static final long CONFIRM_TIMEOUT_SECONDS = 5;

    private final RabbitTemplate rabbitTemplate;
    private final JsonMapper json;

    public RabbitOutboxDispatcher(RabbitTemplate rabbitTemplate, JsonMapper json) {
        this.rabbitTemplate = rabbitTemplate;
        this.json = json;
    }

    @Override
    public boolean supports(OutboxMessage message) {
        return OutboxWriter.KIND_EVENT.equals(message.kind());
    }

    @Override
    public void dispatch(OutboxMessage message) throws Exception {
        JsonNode payload = json.readTree(message.payload());
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        props.setContentEncoding(StandardCharsets.UTF_8.name());
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        String messageId = payload.path(MessagingNames.FIELD_MESSAGE_ID).asString(null);
        props.setMessageId(messageId);
        props.setHeader(MessageHeaders.MESSAGE_ID, messageId);
        props.setHeader(MessageHeaders.SCHEMA_VERSION, payload.path(MessagingNames.FIELD_SCHEMA_VERSION).asString("1"));
        props.setHeader(MessageHeaders.SCHEMA, message.routingKey());
        props.setHeader(MessageHeaders.ORGANIZATION_ID, Long.toString(message.organizationId()));
        String occurredAt = payload.path("occurredAt").asString(null);
        if (occurredAt != null) {
            props.setHeader(MessageHeaders.OCCURRED_AT, occurredAt);
        }
        String requestId = payload.path("requestId").asString(null);
        if (requestId != null) {
            props.setHeader(DataflowHeaders.REQUEST_ID, requestId);
        }
        CorrelationData correlation = new CorrelationData(Long.toString(message.id()));
        rabbitTemplate.send(message.exchange(), message.routingKey(),
                new Message(message.payload().getBytes(StandardCharsets.UTF_8), props), correlation);
        CorrelationData.Confirm confirm = correlation.getFuture().get(CONFIRM_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!confirm.ack()) {
            throw new IllegalStateException("RabbitMQ nack: " + confirm.reason());
        }
        if (correlation.getReturned() != null) {
            throw new IllegalStateException("RabbitMQ returned: " + correlation.getReturned().getReplyText());
        }
    }
}
