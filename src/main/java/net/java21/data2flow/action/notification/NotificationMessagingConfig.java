package net.java21.data2flow.action.notification;

import net.java21.data2flow.action.notification.event.AlarmEventListener;
import net.java21.data2flow.action.notification.event.NotificationConfigListener;
import net.java21.data2flow.action.notification.event.NotifyRequestListener;
import net.java21.data2flow.action.notification.service.EscalationService;
import net.java21.data2flow.action.notification.service.NotificationCoreClient;
import net.java21.data2flow.action.notification.service.NotificationService;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.QuorumQueueSpec;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.MessageListener;
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
 * 알림 메시징(ADR-020·025·048):
 * <ul>
 *   <li>{@code data2flow.actions}(direct) · {@code notify} → {@code action.notifications}(Quorum, delivery-limit 5, DLX → {@code .dlq})</li>
 *   <li>{@code data2flow.events}(topic) · {@code alarm.acked}·{@code alarm.cleared} → {@code action.notification.events}(에스컬레이션 취소)</li>
 *   <li>{@code data2flow.config}(fanout) → 인스턴스별 임시 큐 {@code action.notification-config.*}(정의 캐시 무효화)</li>
 * </ul>
 * 수동 ACK(DB 커밋 뒤)이고 종료 때 처리 중인 메시지를 마친다. 시험·로컬에서 끈다({@code data2flow.action.messaging.enabled=false}).
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "data2flow.action.messaging", name = "enabled", havingValue = "true", matchIfMissing = true)
public class NotificationMessagingConfig {

    static final QuorumQueueSpec ALARM_EVENTS = QuorumQueueSpec.events("action.notification");

    @Bean
    Declarables notificationDeclarables() {
        List<Declarable> d = new ArrayList<>();
        DirectExchange actions = new DirectExchange(MessagingNames.EXCHANGE_ACTIONS, true, false);
        TopicExchange events = new TopicExchange(MessagingNames.EXCHANGE_EVENTS, true, false);
        DirectExchange dlx = new DirectExchange(MessagingNames.EXCHANGE_DLX, true, false);
        d.add(actions);
        d.add(events);
        d.add(dlx);
        QuorumQueueSpec notify = QuorumQueueSpec.ACTION_NOTIFICATIONS;
        Queue queue = new Queue(notify.name(), true, false, false, notify.arguments());
        Queue dlq = new Queue(notify.deadLetterQueue(), true, false, false, QuorumQueueSpec.deadLetterArguments());
        d.add(queue);
        d.add(dlq);
        d.add(BindingBuilder.bind(queue).to(actions).with(notify.routingKey()));
        d.add(BindingBuilder.bind(dlq).to(dlx).with(notify.name()));
        Queue alarmQueue = new Queue(ALARM_EVENTS.name(), true, false, false, ALARM_EVENTS.arguments());
        Queue alarmDlq = new Queue(ALARM_EVENTS.deadLetterQueue(), true, false, false, QuorumQueueSpec.deadLetterArguments());
        d.add(alarmQueue);
        d.add(alarmDlq);
        for (String key : AlarmEventListener.ROUTING_KEYS) {
            d.add(BindingBuilder.bind(alarmQueue).to(events).with(key));
        }
        d.add(BindingBuilder.bind(alarmDlq).to(dlx).with(ALARM_EVENTS.name()));
        return new Declarables(d);
    }

    @Bean
    NotifyRequestListener notifyRequestListener(NotificationService service) {
        return new NotifyRequestListener(service);
    }

    @Bean
    SimpleMessageListenerContainer notifyRequestContainer(ConnectionFactory cf, NotifyRequestListener listener) {
        return container(cf, QuorumQueueSpec.ACTION_NOTIFICATIONS.name(), QuorumQueueSpec.ACTION_NOTIFICATIONS.prefetch(), listener);
    }

    @Bean
    AlarmEventListener notificationAlarmEventListener(EscalationService escalations) {
        return new AlarmEventListener(escalations);
    }

    @Bean
    SimpleMessageListenerContainer notificationAlarmEventContainer(ConnectionFactory cf, AlarmEventListener listener) {
        return container(cf, ALARM_EVENTS.name(), ALARM_EVENTS.prefetch(), listener);
    }

    @Bean
    AnonymousQueue notificationConfigQueue() {
        return new AnonymousQueue(new Base64UrlNamingStrategy("action.notification-config."));
    }

    @Bean
    Declarables notificationConfigBinding(AnonymousQueue notificationConfigQueue) {
        FanoutExchange config = new FanoutExchange(MessagingNames.EXCHANGE_CONFIG, true, false);
        return new Declarables(config, BindingBuilder.bind(notificationConfigQueue).to(config));
    }

    @Bean
    NotificationConfigListener notificationConfigListener(NotificationCoreClient core) {
        return new NotificationConfigListener(core);
    }

    @Bean
    SimpleMessageListenerContainer notificationConfigContainer(ConnectionFactory cf, AnonymousQueue notificationConfigQueue,
                                                               NotificationConfigListener listener, NotificationCoreClient core) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
        c.setQueues(notificationConfigQueue);
        c.setAcknowledgeMode(AcknowledgeMode.AUTO);
        c.setMessageListener(listener);
        c.setMissingQueuesFatal(false);
        cf.addConnectionListener(new org.springframework.amqp.rabbit.connection.ConnectionListener() {
            @Override
            public void onCreate(org.springframework.amqp.rabbit.connection.Connection connection) {
                core.invalidateAll();   // 재연결: 놓친 변경을 원천에서 다시 읽는다
            }
        });
        return c;
    }

    private static SimpleMessageListenerContainer container(ConnectionFactory cf, String queue, int prefetch, Object listener) {
        SimpleMessageListenerContainer c = new SimpleMessageListenerContainer(cf);
        c.setQueueNames(queue);
        c.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        c.setPrefetchCount(prefetch);
        c.setConcurrentConsumers(1);
        c.setDefaultRequeueRejected(true);
        c.setMissingQueuesFatal(false);
        c.setShutdownTimeout(20_000);
        c.setMessageListener((MessageListener) listener);
        return c;
    }
}
