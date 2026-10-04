package net.java21.data2flow.action.sink;

import net.java21.data2flow.action.sink.event.SinkConfigListener;
import net.java21.data2flow.action.sink.event.SinkRequestListener;
import net.java21.data2flow.action.sink.service.SinkConnectionCache;
import net.java21.data2flow.action.sink.service.SinkWriteService;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.QuorumQueueSpec;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Sink 메시징(ADR-020): {@code data2flow.actions} · {@code sink} → {@code action.sinks}(Quorum, delivery-limit 5, DLX →
 * {@code action.sinks.dlq}) 소비(수동 ACK), {@code data2flow.config} fanout → 인스턴스별 임시 큐 {@code action.sink-config.*}.
 * 시험·로컬에서 끈다({@code data2flow.action.messaging.enabled=false}).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "data2flow.action.messaging", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SinkMessagingConfig {

    @Bean
    Declarables sinkDeclarables() {
        QuorumQueueSpec spec = QuorumQueueSpec.ACTION_SINKS;
        DirectExchange actions = new DirectExchange(MessagingNames.EXCHANGE_ACTIONS, true, false);
        DirectExchange dlx = new DirectExchange(MessagingNames.EXCHANGE_DLX, true, false);
        Queue queue = new Queue(spec.name(), true, false, false, spec.arguments());
        Queue dlq = new Queue(spec.deadLetterQueue(), true, false, false, QuorumQueueSpec.deadLetterArguments());
        return new Declarables(actions, dlx, queue, dlq, BindingBuilder.bind(queue).to(actions).with(spec.routingKey()),
                BindingBuilder.bind(dlq).to(dlx).with(spec.name()));
    }

    @Bean
    SinkRequestListener sinkRequestListener(SinkWriteService service) {
        return new SinkRequestListener(service);
    }

    @Bean
    SimpleMessageListenerContainer sinkRequestContainer(ConnectionFactory cf, SinkRequestListener listener) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
        c.setQueueNames(QuorumQueueSpec.ACTION_SINKS.name());
        c.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        c.setPrefetchCount(QuorumQueueSpec.ACTION_SINKS.prefetch());
        c.setConcurrentConsumers(1);
        c.setDefaultRequeueRejected(true);
        c.setMissingQueuesFatal(false);
        c.setShutdownTimeout(20_000);
        c.setMessageListener(listener);
        return c;
    }

    @Bean
    AnonymousQueue sinkConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("action.sink-config."));
    }

    @Bean
    Binding sinkConfigBinding(AnonymousQueue sinkConfigQueue) {
        return BindingBuilder.bind(sinkConfigQueue).to(new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false));
    }

    @Bean
    SinkConfigListener sinkConfigListener(SinkConnectionCache cache) {
        return new SinkConfigListener(cache);
    }

    @Bean
    SimpleMessageListenerContainer sinkConfigContainer(ConnectionFactory cf, AnonymousQueue sinkConfigQueue, SinkConfigListener listener) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
        c.setQueues(sinkConfigQueue);
        c.setAcknowledgeMode(AcknowledgeMode.AUTO);
        c.setMessageListener(listener);
        c.setMissingQueuesFatal(false);
        cf.addConnectionListener(new org.springframework.amqp.rabbit.connection.ConnectionListener() {
            @Override
            public void onCreate(org.springframework.amqp.rabbit.connection.Connection connection) {
                listener.invalidateAll();
            }
        });
        return c;
    }
}
