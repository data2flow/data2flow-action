package net.java21.data2flow.action.output;

import net.java21.data2flow.action.output.event.OutputConfigListener;
import net.java21.data2flow.action.output.event.OutputStreamConnection;
import net.java21.data2flow.action.output.event.OutputTelemetryConsumer;
import net.java21.data2flow.action.output.service.DeviceContextCache;
import net.java21.data2flow.action.output.service.OutputConnectionRegistry;
import net.java21.data2flow.action.output.service.OutputEnqueueService;
import net.java21.data2flow.contracts.messaging.ConsumerGroups;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.amqp.autoconfigure.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 출력 연결 메시징: {@code data2flow.config} fanout → 임시 큐 {@code action.output-config.*}(설정 변경), 그리고
 * {@code data2flow.action.output.consumer-enabled=true}일 때 {@code data2flow.telemetry} Super Stream 소비(그룹 {@code action-output}).
 * 시험·로컬에서는 끈다.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "data2flow.action.messaging", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OutputMessagingConfig {

    @Bean
    AnonymousQueue outputConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("action.output-config."));
    }

    @Bean
    Binding outputConfigBinding(AnonymousQueue outputConfigQueue) {
        return BindingBuilder.bind(outputConfigQueue).to(new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false));
    }

    @Bean
    OutputConfigListener outputConfigListener(OutputConnectionRegistry registry, DeviceContextCache contexts) {
        return new OutputConfigListener(registry, contexts);
    }

    @Bean
    SimpleMessageListenerContainer outputConfigContainer(ConnectionFactory cf, AnonymousQueue outputConfigQueue,
                                                         OutputConfigListener listener) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
        c.setQueues(outputConfigQueue);
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

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "data2flow.action.output", name = "consumer-enabled", havingValue = "true")
    OutputStreamConnection outputStreamConnection(RabbitProperties rabbit, OutputProperties p) {
        return new OutputStreamConnection(rabbit, p.streamPort(), p.streamFixedAddress());
    }

    @Bean
    @ConditionalOnProperty(prefix = "data2flow.action.output", name = "consumer-enabled", havingValue = "true")
    OutputTelemetryConsumer outputTelemetryConsumer(OutputStreamConnection connection, OutputEnqueueService enqueue, OutputProperties p) {
        return new OutputTelemetryConsumer(connection, enqueue, ConsumerGroups.of(ConsumerGroups.ACTION_OUTPUT, p.developer()));
    }
}
