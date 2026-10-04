package net.java21.data2flow.action.notification;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.notification.channel.ChannelRegistry;
import net.java21.data2flow.action.notification.channel.telegram.TelegramChannel;
import net.java21.data2flow.action.notification.domain.NotificationRetryPolicy;
import net.java21.data2flow.action.notification.repository.AggregateRepository;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.notification.repository.EscalationRepository;
import net.java21.data2flow.action.notification.service.Aggregator;
import net.java21.data2flow.action.notification.service.ChannelAdminService;
import net.java21.data2flow.action.notification.service.DeliveryDispatcher;
import net.java21.data2flow.action.notification.service.EscalationService;
import net.java21.data2flow.action.notification.service.MessengerCallbackService;
import net.java21.data2flow.action.notification.service.NotificationCoreClient;
import net.java21.data2flow.action.notification.service.NotificationService;
import net.java21.data2flow.action.notification.service.RecipientResolver;
import net.java21.data2flow.action.outbox.OutboxRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * 알림 공통 계층 빈(OPS-06.06, RUL-03, ADR-033). 채널은 {@link NotificationChannel} 빈으로 등록한다(지금 텔레그램 하나, 기본 꺼짐).
 * 메시징은 {@link NotificationMessagingConfig}, 주기 작업은 {@link Jobs}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(NotificationProperties.class)
public class NotificationConfig {

    @Bean
    NotificationCoreClient notificationCoreClient(ActionProperties actionProperties, NotificationProperties properties, Clock clock) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(2));
        factory.setReadTimeout(Duration.ofSeconds(5));
        return new NotificationCoreClient(RestClient.builder().baseUrl(actionProperties.coreUri()).requestFactory(factory).build(), clock,
                properties.cacheTtl());
    }

    /** 텔레그램 채널(OPS-06.01). {@code data2flow.action.notification.telegram.enabled=false}(기본)면 쓸 수 없음으로 보고된다 */
    @Bean
    TelegramChannel telegramChannel(NotificationProperties properties) {
        return new TelegramChannel(properties.telegram());
    }

    @Bean
    ChannelRegistry notificationChannelRegistry(List<NotificationChannel> channels) {
        return new ChannelRegistry(channels);
    }

    @Bean
    NotificationRetryPolicy notificationRetryPolicy(NotificationProperties properties) {
        return new NotificationRetryPolicy(properties.retryBackoffs());
    }

    @Bean
    RecipientResolver recipientResolver(NotificationCoreClient core) {
        return new RecipientResolver(core);
    }

    @Bean
    DeliveryDispatcher deliveryDispatcher(DeliveryRepository deliveries, NotificationCoreClient core, ChannelRegistry channels,
                                          NotificationRetryPolicy retry, OutboxWriter outbox, PlatformTransactionManager tx,
                                          NotificationProperties properties, ActionProperties actionProperties, MeterRegistry meters,
                                          Clock clock) {
        return new DeliveryDispatcher(deliveries, core, channels, retry, outbox, tx, properties, actionProperties, meters, clock);
    }

    @Bean
    NotificationService notificationService(NotificationCoreClient core, RecipientResolver resolver, DeliveryRepository deliveries,
                                            AggregateRepository aggregates, EscalationRepository escalations, ExecutionRepository executions,
                                            DeliveryDispatcher dispatcher, PlatformTransactionManager tx, NotificationProperties properties,
                                            ActionProperties actionProperties, Clock clock) {
        return new NotificationService(core, resolver, deliveries, aggregates, escalations, executions, dispatcher, tx, properties,
                actionProperties, clock);
    }

    @Bean
    Aggregator notificationAggregator(AggregateRepository aggregates, DeliveryRepository deliveries, DeliveryDispatcher dispatcher,
                                      PlatformTransactionManager tx, NotificationProperties properties, ActionProperties actionProperties,
                                      Clock clock) {
        return new Aggregator(aggregates, deliveries, dispatcher, tx, properties, actionProperties, clock);
    }

    @Bean
    EscalationService escalationService(EscalationRepository escalations, NotificationCoreClient core, NotificationService notifications,
                                        DeliveryDispatcher dispatcher, OutboxRepository outbox, PlatformTransactionManager tx,
                                        NotificationProperties properties, ActionProperties actionProperties, Clock clock) {
        return new EscalationService(escalations, core, notifications, dispatcher, outbox, tx, properties, actionProperties, clock);
    }

    @Bean
    MessengerCallbackService messengerCallbackService(ChannelRegistry channels, NotificationCoreClient core, DeliveryRepository deliveries) {
        return new MessengerCallbackService(channels, core, deliveries);
    }

    @Bean
    ChannelAdminService channelAdminService(ChannelRegistry channels, NotificationCoreClient core, DeliveryRepository deliveries,
                                            DeliveryDispatcher dispatcher, PlatformTransactionManager tx, NotificationProperties properties,
                                            ActionProperties actionProperties, Clock clock) {
        return new ChannelAdminService(channels, core, deliveries, dispatcher, tx, properties, actionProperties, clock);
    }

    /**
     * 주기 작업(1초): 재시도·대기 해제 발송, 묶음 창 닫기, 에스컬레이션. 여러 파드가 함께 돌아도 {@code FOR UPDATE SKIP LOCKED}로 한 행은
     * 한 파드만 처리한다. 시험은 {@code data2flow.action.scheduler.enabled=false}로 끄고 직접 부른다.
     */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class Jobs {

        private static final Logger log = LoggerFactory.getLogger(Jobs.class);
        private final DeliveryDispatcher dispatcher;
        private final Aggregator aggregator;
        private final EscalationService escalations;

        Jobs(DeliveryDispatcher dispatcher, Aggregator aggregator, EscalationService escalations) {
            this.dispatcher = dispatcher;
            this.aggregator = aggregator;
            this.escalations = escalations;
        }

        @Scheduled(fixedDelayString = "${data2flow.action.scheduler.period:1s}")
        void run() {
            try {
                aggregator.flushDue();
                escalations.processDue();
                dispatcher.processDue();
            } catch (RuntimeException e) {
                log.warn("알림 주기 작업 실패(다음 주기에 다시): {}", e.toString());
            }
        }
    }
}
