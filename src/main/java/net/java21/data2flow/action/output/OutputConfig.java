package net.java21.data2flow.action.output;

import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.output.domain.RetryPolicy;
import net.java21.data2flow.action.output.repository.OutputDeliveryRepository;
import net.java21.data2flow.action.output.service.DeviceContextCache;
import net.java21.data2flow.action.output.service.OutputConnectionRegistry;
import net.java21.data2flow.action.output.service.OutputCoreClient;
import net.java21.data2flow.action.output.service.OutputDeliveryService;
import net.java21.data2flow.action.output.service.OutputEnqueueService;
import net.java21.data2flow.action.output.service.OutputStats;
import net.java21.data2flow.action.output.service.OutputTestService;
import net.java21.data2flow.action.output.transport.MqttOutputPublisher;
import net.java21.data2flow.action.output.transport.WebhookOutputSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Duration;

/**
 * 출력 연결 패키지 빈(DSC-04.01, ADR-048): core 정의·기기 맥락(API-DSC-73·74), 대기열 쌓기, 연결별 순서 발송(MQTT·Webhook), 1분 지표
 * (API-DSC-75), 연결 테스트. 스트림 소비·설정 수신은 {@link OutputMessagingConfig}, 주기 작업은 {@link OutputJobs}.
 * actuation·notification과 분리된 패키지다(M5, milestones.md).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OutputProperties.class)
public class OutputConfig {

    @Bean
    OutputCoreClient outputCoreClient(ActionProperties action) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(2));
        factory.setReadTimeout(Duration.ofSeconds(10));
        return new OutputCoreClient(RestClient.builder().baseUrl(action.coreUri()).requestFactory(factory).build());
    }

    @Bean(destroyMethod = "close")
    MqttOutputPublisher mqttOutputPublisher(OutputProperties p, ActionProperties action) {
        // client-id 앞부분(실제 ID는 연결마다 -out<id>): data2flow-action-<env>-<파드>
        String base = "data2flow-action-" + action.env() + "-" + action.instanceId().replaceAll("[^A-Za-z0-9-]", "-");
        return new MqttOutputPublisher(p.deniedHosts(), base, p.testTimeout());
    }

    @Bean
    WebhookOutputSender webhookOutputSender(OutputProperties p, Clock clock) {
        return new WebhookOutputSender(p.deniedHosts(), p.testTimeout(), clock);
    }

    @Bean
    OutputConnectionRegistry outputConnectionRegistry(OutputCoreClient core, OutputProperties p, Clock clock) {
        // 정의가 바뀌면 MQTT 접속은 버전 비교로 다시 만든다(publish 때)
        return new OutputConnectionRegistry(core, clock, p.runtimeRefresh(), p.deniedHosts(), null);
    }

    @Bean
    DeviceContextCache deviceContextCache(OutputCoreClient core, OutputProperties p, Clock clock) {
        return new DeviceContextCache(core, clock, p.contextTtl());
    }

    @Bean
    OutputStats outputStats(OutputCoreClient core, Clock clock) {
        return new OutputStats(core, clock);
    }

    @Bean
    OutputEnqueueService outputEnqueueService(OutputConnectionRegistry registry, DeviceContextCache contexts,
                                              OutputDeliveryRepository deliveries, PlatformTransactionManager tx, Clock clock,
                                              ActionProperties action) {
        return new OutputEnqueueService(registry, contexts, deliveries, new TransactionTemplate(tx), clock, action.env());
    }

    @Bean
    OutputDeliveryService outputDeliveryService(OutputConnectionRegistry registry, OutputDeliveryRepository deliveries,
                                                MqttOutputPublisher mqtt, WebhookOutputSender webhook, OutputStats stats,
                                                OutputProperties p, Clock clock, ActionProperties action) {
        return new OutputDeliveryService(registry, deliveries, mqtt, webhook, stats,
                new RetryPolicy(p.retryInitial(), p.retryMax(), p.maxRetryAge()), clock, action.env(), action.instanceId(), p.lease(),
                p.batch());
    }

    @Bean
    OutputTestService outputTestService(MqttOutputPublisher mqtt, WebhookOutputSender webhook, OutputProperties p) {
        return new OutputTestService(mqtt, webhook, p.deniedHosts());
    }

    /** 발송(0.5초)·지표 전송(1분)·보관 정리(1시간). 종료할 때 지표를 마저 보내고 리스를 놓는다. 시험은 끄고 직접 부른다 */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.output", name = "sender-enabled", havingValue = "true", matchIfMissing = true)
    static class OutputJobs implements DisposableBean {

        private static final Logger log = LoggerFactory.getLogger(OutputJobs.class);
        private final OutputDeliveryService deliveries;
        private final OutputStats stats;
        private final OutputDeliveryRepository repository;
        private final OutputProperties properties;
        private final Clock clock;
        private volatile boolean stopped;

        OutputJobs(OutputDeliveryService deliveries, OutputStats stats, OutputDeliveryRepository repository, OutputProperties properties,
                   Clock clock) {
            this.deliveries = deliveries;
            this.stats = stats;
            this.repository = repository;
            this.properties = properties;
            this.clock = clock;
        }

        @Scheduled(fixedDelayString = "${data2flow.action.output.sender-period:500ms}")
        void deliver() {
            if (stopped) {
                return;
            }
            try {
                deliveries.deliverDue();
            } catch (RuntimeException e) {
                log.warn("출력 발송 작업 실패(다음 주기에 다시): {}", e.toString());
            }
        }

        @Scheduled(fixedDelayString = "PT1M", initialDelayString = "PT1M")
        void flushStats() {
            stats.flush(false);
        }

        @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT5M")
        void purge() {
            try {
                repository.deleteExpired(clock.instant().minus(properties.retention()));
            } catch (RuntimeException e) {
                log.warn("출력 대기열 보관 정리 실패: {}", e.toString());
            }
        }

        @Override
        public void destroy() {
            stopped = true;
            stats.flush(true);
            try {
                deliveries.releaseLeases();
            } catch (RuntimeException e) {
                log.debug("출력 발송 리스 해제 실패: {}", e.toString());
            }
        }
    }
}
