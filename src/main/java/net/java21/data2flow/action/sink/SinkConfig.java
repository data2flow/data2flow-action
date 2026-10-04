package net.java21.data2flow.action.sink;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.sink.connector.SinkConnector;
import net.java21.data2flow.action.sink.connector.SinkConnectorRegistry;
import net.java21.data2flow.action.sink.connector.influx.InfluxSinkConnector;
import net.java21.data2flow.action.sink.connector.jdbc.MySqlSinkConnector;
import net.java21.data2flow.action.sink.connector.jdbc.PostgresSinkConnector;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository;
import net.java21.data2flow.action.sink.service.SinkConnectionCache;
import net.java21.data2flow.action.sink.service.SinkConnectionClient;
import net.java21.data2flow.action.sink.service.SinkConnectionService;
import net.java21.data2flow.action.sink.service.SinkRetryPolicy;
import net.java21.data2flow.action.sink.service.SinkWriteService;
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
 * Sink 패키지 빈(FLW-04, ADR-025): 커넥터(PostgreSQL·MySQL·InfluxDB)와 등록부, 연결 정의 캐시(core API-FLW-85), 쓰기·재시도 서비스.
 * 큐 소비·설정 수신은 {@link SinkMessagingConfig}, 재시도·정리 작업은 {@link SinkJobs}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(SinkProperties.class)
public class SinkConfig {

    @Bean
    PostgresSinkConnector postgresSinkConnector(SinkProperties p, Clock clock) {
        return new PostgresSinkConnector(clock, p.poolSize(), p.connectTimeout());
    }

    @Bean
    MySqlSinkConnector mySqlSinkConnector(SinkProperties p, Clock clock) {
        return new MySqlSinkConnector(clock, p.poolSize(), p.connectTimeout());
    }

    @Bean
    InfluxSinkConnector influxSinkConnector(SinkProperties p, Clock clock) {
        return new InfluxSinkConnector(clock, p.connectTimeout());
    }

    @Bean(destroyMethod = "closeAll")
    SinkConnectorRegistry sinkConnectorRegistry(List<SinkConnector> connectors) {
        return new SinkConnectorRegistry(connectors);
    }

    @Bean
    SinkConnectionCache sinkConnectionCache(ActionProperties action, SinkProperties p, SinkConnectorRegistry registry, Clock clock) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(2));
        factory.setReadTimeout(Duration.ofSeconds(5));
        RestClient core = RestClient.builder().baseUrl(action.coreUri()).requestFactory(factory).build();
        return new SinkConnectionCache(new SinkConnectionClient(core), registry, clock, p.connectionTtl());
    }

    @Bean
    SinkWriteService sinkWriteService(SinkBatchRepository batches, ExecutionRepository executions, SinkConnectionCache cache,
                                      SinkConnectorRegistry registry, SinkProperties p, PlatformTransactionManager tx,
                                      MeterRegistry meters, Clock clock, ActionProperties action) {
        return new SinkWriteService(batches, executions, cache, registry, SinkRetryPolicy.of(p), tx, meters, clock, action.env(),
                p.lease(), p.writtenRetention());
    }

    @Bean
    SinkConnectionService sinkConnectionService(SinkConnectionCache cache, SinkConnectorRegistry registry, SinkBatchRepository batches,
                                                SinkWriteService writer) {
        return new SinkConnectionService(cache, registry, batches, writer);
    }

    /** Sink 재시도(1초)와 보관 정리(1시간). 시험은 끄고 직접 부른다 */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class SinkJobs {

        private static final Logger log = LoggerFactory.getLogger(SinkJobs.class);
        private final SinkWriteService writer;
        private final SinkProperties properties;

        SinkJobs(SinkWriteService writer, SinkProperties properties) {
            this.writer = writer;
            this.properties = properties;
        }

        @Scheduled(fixedDelayString = "${data2flow.action.scheduler.period:1s}")
        void retry() {
            try {
                writer.processDue(properties.batch());
            } catch (RuntimeException e) {
                log.warn("Sink 재시도 작업 실패(다음 주기에 다시): {}", e.toString());
            }
        }

        @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT2M")
        void purge() {
            try {
                writer.purge();
            } catch (RuntimeException e) {
                log.warn("Sink 보관 정리 실패: {}", e.toString());
            }
        }
    }
}
