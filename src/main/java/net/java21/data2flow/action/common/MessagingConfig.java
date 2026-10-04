package net.java21.data2flow.action.common;

import net.java21.data2flow.action.actuation.event.ActionCommandListener;
import net.java21.data2flow.action.actuation.event.ConfigChangeListener;
import net.java21.data2flow.action.actuation.event.DeviceEventListener;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.service.ActionRequestHandler;
import net.java21.data2flow.action.actuation.service.CommandTracker;
import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.action.actuation.service.SandboxRegistry;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.QuorumQueueSpec;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * RabbitMQ(ADR-020, architecture.md §4.3·4.5): 큐·바인딩 선언과 소비자.
 *
 * <ul>
 *   <li>{@code data2flow.actions}(direct) · {@code command} → {@code action.commands}(Quorum, delivery-limit 5, DLX → {@code action.commands.dlq}), prefetch 20</li>
 *   <li>{@code data2flow.events}(topic) → {@code action.events}: {@code device.command.ack}, {@code device.state.reported},
 *       {@code device.connectivity.changed}, {@code device.changed}, {@code control.emergency.started|released}</li>
 *   <li>{@code data2flow.config}(fanout) → 인스턴스별 임시 큐 {@code action.config.*}</li>
 * </ul>
 * 소비자는 수동 ACK(DB 커밋 뒤)이고, 종료 때 처리 중인 메시지를 마친 뒤 멈춘다(graceful shutdown, reliability-and-ha.md §4).
 * 시험·로컬에서 끌 수 있다({@code data2flow.action.messaging.enabled=false}).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "data2flow.action.messaging", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MessagingConfig {

    static final QuorumQueueSpec EVENTS = QuorumQueueSpec.events("action");

    @Bean
    Declarables actionDeclarables() {
        List<Declarable> d = new ArrayList<>();
        DirectExchange actions = new DirectExchange(MessagingNames.EXCHANGE_ACTIONS, true, false);
        TopicExchange events = new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false);
        DirectExchange dlx = new DirectExchange(MessagingNames.EXCHANGE_DLX, true, false);
        d.add(actions);
        d.add(events);
        d.add(dlx);
        QuorumQueueSpec commands = QuorumQueueSpec.ACTION_COMMANDS;
        Queue commandQueue = new Queue(commands.name(), true, false, false, commands.arguments());
        Queue commandDlq = new Queue(commands.deadLetterQueue(), true, false, false, QuorumQueueSpec.deadLetterArguments());
        d.add(commandQueue);
        d.add(commandDlq);
        d.add(BindingBuilder.bind(commandQueue).to(actions).with(commands.routingKey()));
        d.add(BindingBuilder.bind(commandDlq).to(dlx).with(commands.name()));
        Queue eventQueue = new Queue(EVENTS.name(), true, false, false, EVENTS.arguments());
        Queue eventDlq = new Queue(EVENTS.deadLetterQueue(), true, false, false, QuorumQueueSpec.deadLetterArguments());
        d.add(eventQueue);
        d.add(eventDlq);
        for (String key : DeviceEventListener.ROUTING_KEYS) {
            d.add(BindingBuilder.bind(eventQueue).to(events).with(key));
        }
        d.add(BindingBuilder.bind(eventDlq).to(dlx).with(EVENTS.name()));
        return new Declarables(d);
    }

    // ───────────── action.commands ─────────────

    @Bean
    ActionCommandListener actionCommandListener(ActionRequestHandler handler) {
        return new ActionCommandListener(handler);
    }

    @Bean
    SimpleMessageListenerContainer actionCommandContainer(ConnectionFactory connectionFactory, ActionCommandListener listener) {
        return container(connectionFactory, QuorumQueueSpec.ACTION_COMMANDS.name(), QuorumQueueSpec.ACTION_COMMANDS.prefetch(), listener);
    }

    // ───────────── action.events ─────────────

    @Bean
    DeviceEventListener deviceEventListener(CommandTracker tracker, ShadowRepository shadows, ControlProfileService profiles,
                                            net.java21.data2flow.action.actuation.service.EmergencyStopHandler emergency) {
        return new DeviceEventListener(tracker, shadows, profiles, emergency);
    }

    @Bean
    SimpleMessageListenerContainer deviceEventContainer(ConnectionFactory connectionFactory, DeviceEventListener listener) {
        return container(connectionFactory, EVENTS.name(), EVENTS.prefetch(), listener);
    }

    // ───────────── data2flow.config ─────────────

    @Bean
    FanoutExchange configExchange() {
        return new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false);
    }

    @Bean
    AnonymousQueue actionConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("action.config."));
    }

    @Bean
    Binding actionConfigBinding(AnonymousQueue actionConfigQueue, FanoutExchange configExchange) {
        return BindingBuilder.bind(actionConfigQueue).to(configExchange);
    }

    @Bean
    ConfigChangeListener configChangeListener(ControlProfileService profiles, SandboxRegistry sandbox,
                                              net.java21.data2flow.action.actuation.service.InterlockService interlocks,
                                              net.java21.data2flow.action.actuation.service.EmergencyStopRegistry emergency) {
        return new ConfigChangeListener(profiles, sandbox, interlocks, emergency);
    }

    @Bean
    SimpleMessageListenerContainer configChangeContainer(ConnectionFactory connectionFactory, AnonymousQueue actionConfigQueue,
                                                         ConfigChangeListener listener) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(connectionFactory);
        c.setQueues(actionConfigQueue);
        c.setAcknowledgeMode(AcknowledgeMode.AUTO);
        c.setMessageListener(listener);
        c.setMissingQueuesFatal(false);
        connectionFactory.addConnectionListener(new org.springframework.amqp.rabbit.connection.ConnectionListener() {
            @Override
            public void onCreate(org.springframework.amqp.rabbit.connection.Connection connection) {
                listener.invalidateAll();   // 재연결: 놓친 설정 변경을 원천에서 다시 읽게 한다
            }
        });
        return c;
    }

    private static SimpleMessageListenerContainer container(ConnectionFactory cf, String queue, int prefetch,
                                                            org.springframework.amqp.core.MessageListener listener) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
        c.setQueueNames(queue);
        c.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        c.setPrefetchCount(prefetch);
        c.setConcurrentConsumers(1);
        c.setDefaultRequeueRejected(true);
        c.setMissingQueuesFatal(false);
        c.setShutdownTimeout(20_000);
        c.setMessageListener(listener);
        return c;
    }
}
