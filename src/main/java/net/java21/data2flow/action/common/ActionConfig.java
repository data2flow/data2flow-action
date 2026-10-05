package net.java21.data2flow.action.common;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.action.actuation.driver.DeviceDriver;
import net.java21.data2flow.action.actuation.driver.DriverEventSink;
import net.java21.data2flow.action.actuation.driver.DriverRegistry;
import net.java21.data2flow.action.actuation.driver.mqtt.MqttDriver;
import net.java21.data2flow.action.actuation.driver.virtual.VirtualDriver;
import net.java21.data2flow.action.actuation.repository.CommandEventRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.DeviceStateRepository;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.service.ActionRequestHandler;
import net.java21.data2flow.action.actuation.service.CommandDispatcher;
import net.java21.data2flow.action.actuation.service.CommandEvents;
import net.java21.data2flow.action.actuation.service.CommandQueryService;
import net.java21.data2flow.action.actuation.service.CommandTracker;
import net.java21.data2flow.action.actuation.service.CommandWaiter;
import net.java21.data2flow.action.actuation.service.ControlFacade;
import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.action.actuation.service.SandboxRegistry;
import net.java21.data2flow.action.actuation.service.BulkControlService;
import net.java21.data2flow.action.actuation.service.ControlEffectService;
import net.java21.data2flow.action.actuation.service.DriverHealthService;
import net.java21.data2flow.action.actuation.service.EmergencyStopHandler;
import net.java21.data2flow.action.actuation.service.EmergencyStopRegistry;
import net.java21.data2flow.action.actuation.service.InterlockService;
import net.java21.data2flow.action.actuation.service.RuntimeStatsService;
import net.java21.data2flow.action.actuation.service.SceneService;
import net.java21.data2flow.action.actuation.repository.DriverCallRepository;
import net.java21.data2flow.action.actuation.repository.EffectCheckRepository;
import net.java21.data2flow.action.actuation.repository.RuntimeStatRepository;
import net.java21.data2flow.action.actuation.repository.SceneBulkRepository;
import net.java21.data2flow.action.outbox.CoreCallbackDispatcher;
import net.java21.data2flow.action.outbox.OutboxDispatcher;
import net.java21.data2flow.action.outbox.OutboxRelay;
import net.java21.data2flow.action.outbox.OutboxRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.action.outbox.RabbitOutboxDispatcher;
import net.java21.data2flow.contracts.audit.AuditRecorder;
import net.java21.data2flow.contracts.authz.CachingPermissionLookup;
import net.java21.data2flow.contracts.authz.PermissionLookup;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.messaging.ClientIds;
import org.flywaydb.core.Flyway;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestClient;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

/**
 * action 공통 빈: 시계, Flyway 실행 방식, core·simulator 클라이언트, 제어 창구와 드라이버, 아웃박스.
 * 메시징(큐·이벤트·설정 수신)은 {@link MessagingConfig}, 주기 작업은 {@link ActionJobs}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ActionProperties.class)
@EnableScheduling
public class ActionConfig {

    /** 운영 코드는 이 시계만 쓴다(ArchUnit NO_SYSTEM_CLOCK). 시험은 MutableClock으로 바꾼다 */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** ADR-030: staging과 prod가 DB 하나를 함께 쓰므로 migrate는 staging 배포와 시험에서만. 그 밖(prod·local)은 validate만 */
    @Bean
    FlywayMigrationStrategy flywayMigrationStrategy(ActionProperties properties) {
        return (Flyway flyway) -> {
            if ("migrate".equalsIgnoreCase(properties.flywayMode())) {
                flyway.migrate();
            } else {
                flyway.validate();
            }
        };
    }

    // ───────────── 내부 HTTP(ADR-021: http://data2flow-<svc>, 토큰 없음) ─────────────

    @Bean
    CoreClient coreClient(ActionProperties properties) {
        return new CoreClient(client(properties.coreUri()));
    }

    @Bean
    PermissionLookup permissionLookup(CoreClient core, Clock clock) {
        // BR-IAM-13: 10초 이내 캐시. 장기 토큰 요청은 토큰 ID로 따로 판정·캐시한다(IAM-05.01)
        return new CachingPermissionLookup(PermissionLookup.tokenAware(core::accessGrant), Duration.ofSeconds(10), clock);
    }

    private static RestClient client(String baseUrl) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofSeconds(2));
        factory.setReadTimeout(Duration.ofSeconds(5));
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }

    // ───────────── 드라이버(ACT-03) ─────────────

    @Bean
    VirtualDriver virtualDriver(ActionProperties properties, Clock clock) {
        return new VirtualDriver(client(properties.simulatorUri()), clock);
    }

    /**
     * MQTT 일반 드라이버: {@code data2flow.action.mqtt.enabled=true}일 때만(기본 꺼짐). 공용 브로커 주소는 생성자에서 거부한다(CLAUDE.md §5).
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnProperty(prefix = "data2flow.action.mqtt", name = "enabled", havingValue = "true")
    MqttDriver mqttDriver(ActionProperties properties, ObjectProvider<CommandTracker> tracker, Clock clock) {
        ActionProperties.Mqtt mqtt = properties.mqtt();
        String clientId = ClientIds.of(mqtt.clientIdBase(), properties.env(), ordinal(properties.instanceId()));
        return new MqttDriver(mqtt, clientId, new LazySink(tracker), clock);
    }

    /**
     * LoRaWAN(ChirpStack) 드라이버: {@code data2flow.action.lorawan.enabled=true}일 때만(기본 꺼짐, ⏸ ACT-03.03 결정 대기). 호출할 때마다
     * 공용 ChirpStack·브로커 주소를 거부한다(CLAUDE.md §5).
     */
    @Bean
    @ConditionalOnProperty(prefix = "data2flow.action.lorawan", name = "enabled", havingValue = "true")
    net.java21.data2flow.action.actuation.driver.lorawan.LoRaWanDriver loRaWanDriver(ActionProperties properties,
                                                                                    ObjectProvider<CommandTracker> tracker, Clock clock) {
        return new net.java21.data2flow.action.actuation.driver.lorawan.LoRaWanDriver(properties.lorawan().timeout(),
                properties.lorawan().deniedHosts(), new LazySink(tracker), clock);
    }

    /** LG ThinQ(ACT-03.04): 키 발급 전이라 기본 꺼짐("준비 중", ADR-040) */
    @Bean
    @ConditionalOnProperty(prefix = "data2flow.action.vendors.lg-thinq", name = "enabled", havingValue = "true")
    net.java21.data2flow.action.actuation.driver.vendor.LgThinqDriver lgThinqDriver(ActionProperties properties,
                                                                                   ObjectProvider<CommandTracker> tracker, Clock clock) {
        ActionProperties.Vendor v = properties.vendors().lgThinq();
        return new net.java21.data2flow.action.actuation.driver.vendor.LgThinqDriver(v.baseUrl(), v.timeout(), new LazySink(tracker), clock);
    }

    /** SmartThings(ACT-03.04): 키 발급 전이라 기본 꺼짐("준비 중", ADR-040) */
    @Bean
    @ConditionalOnProperty(prefix = "data2flow.action.vendors.smart-things", name = "enabled", havingValue = "true")
    net.java21.data2flow.action.actuation.driver.vendor.SmartThingsDriver smartThingsDriver(ActionProperties properties,
                                                                                           ObjectProvider<CommandTracker> tracker,
                                                                                           Clock clock) {
        ActionProperties.Vendor v = properties.vendors().smartThings();
        return new net.java21.data2flow.action.actuation.driver.vendor.SmartThingsDriver(v.baseUrl(), v.timeout(), new LazySink(tracker),
                clock);
    }

    @Bean
    DriverRegistry driverRegistry(List<DeviceDriver> drivers) {
        return new DriverRegistry(drivers);
    }

    static int ordinal(String instanceId) {
        int dash = instanceId.lastIndexOf('-');
        try {
            return dash < 0 ? 0 : Integer.parseInt(instanceId.substring(dash + 1));
        } catch (NumberFormatException e) {
            return Math.floorMod(instanceId.hashCode(), 1000);
        }
    }

    /** 드라이버 → 추적기 순환 의존을 끊는다(추적기는 드라이버 호출기를, 드라이버는 추적기를 가리킴) */
    record LazySink(ObjectProvider<CommandTracker> tracker) implements DriverEventSink {
        @Override
        public void ack(long organizationId, net.java21.data2flow.contracts.message.event.DeviceCommandAck ack) {
            tracker.getObject().ack(organizationId, ack);
        }

        @Override
        public void reported(long organizationId, net.java21.data2flow.contracts.message.event.DeviceStateReported state) {
            tracker.getObject().reported(organizationId, state);
        }
    }

    // ───────────── 제어 창구(ACT-02) ─────────────

    @Bean
    ControlProfileService controlProfileService(CoreClient core, Clock clock, ActionProperties properties) {
        return new ControlProfileService(core, clock, properties);
    }

    @Bean
    SandboxRegistry sandboxRegistry(CoreClient core) {
        return new SandboxRegistry(core);
    }

    @Bean
    CommandWaiter commandWaiter() {
        return new CommandWaiter();
    }

    @Bean
    DriverHealthService driverHealthService(DriverCallRepository calls, OutboxWriter outbox, PlatformTransactionManager tx,
                                            MeterRegistry meters, Clock clock) {
        return new DriverHealthService(calls, outbox, tx, meters, clock);
    }

    @Bean
    CommandDispatcher commandDispatcher(CommandRepository commands, ShadowRepository shadows, CommandEvents events,
                                        ControlProfileService profiles, DriverRegistry drivers, CommandWaiter waiter,
                                        PlatformTransactionManager tx, ActionProperties properties, MeterRegistry meters, Clock clock,
                                        DriverHealthService health) {
        return new CommandDispatcher(commands, shadows, events, profiles, drivers, waiter, tx, properties, meters, clock, health);
    }

    @Bean
    EmergencyStopRegistry emergencyStopRegistry(CoreClient core, Clock clock, ActionProperties properties) {
        return new EmergencyStopRegistry(core, clock, properties.profileTtl());
    }

    @Bean
    InterlockService interlockService(CoreClient core, ShadowRepository shadows, ControlProfileService profiles, Clock clock,
                                      ActionProperties properties) {
        return new InterlockService(core, shadows, profiles, clock, properties.profileTtl());
    }

    @Bean
    ControlFacade controlFacade(CommandRepository commands, ShadowRepository shadows, DeviceStateRepository deviceState,
                                CommandEvents events, ControlProfileService profiles, SandboxRegistry sandbox, RoleChecker roleChecker,
                                AuditRecorder audit, CommandDispatcher dispatcher, PlatformTransactionManager tx,
                                ActionProperties properties, MeterRegistry meters, Clock clock, EmergencyStopRegistry emergency,
                                InterlockService interlocks) {
        return new ControlFacade(commands, shadows, deviceState, events, profiles, sandbox, roleChecker, audit, dispatcher, tx, properties,
                meters, clock, emergency, interlocks);
    }

    @Bean
    ControlEffectService controlEffectService(EffectCheckRepository checks, RuntimeStatRepository runtime, CoreClient core,
                                              OutboxWriter outbox, PlatformTransactionManager tx, ActionProperties properties,
                                              MeterRegistry meters, Clock clock) {
        return new ControlEffectService(checks, runtime, core, outbox, tx, properties, meters, clock);
    }

    @Bean
    CommandTracker commandTracker(CommandRepository commands, ShadowRepository shadows, DeviceStateRepository deviceState,
                                  CommandEvents events, OutboxWriter outbox, ControlProfileService profiles, CommandDispatcher dispatcher,
                                  ControlFacade facade, CommandWaiter waiter, PlatformTransactionManager tx, ActionProperties properties,
                                  MeterRegistry meters, Clock clock, ControlEffectService effects) {
        return new CommandTracker(commands, shadows, deviceState, events, outbox, profiles, dispatcher, facade, waiter, tx, properties,
                meters, clock, effects);
    }

    @Bean
    EmergencyStopHandler emergencyStopHandler(EmergencyStopRegistry registry, CommandRepository commands, CommandEvents events,
                                              ControlProfileService profiles, PlatformTransactionManager tx, Clock clock) {
        return new EmergencyStopHandler(registry, commands, events, profiles, tx, clock);
    }

    @Bean
    RuntimeStatsService runtimeStatsService(RuntimeStatRepository runtime, EffectCheckRepository effects, ControlProfileService profiles,
                                            ActionProperties properties, Clock clock) {
        return new RuntimeStatsService(runtime, effects, profiles, properties, clock);
    }

    /** 일괄 제어 제출(가상 스레드, ACT-02.06) */
    @Bean(destroyMethod = "close")
    java.util.concurrent.ExecutorService actionBackgroundExecutor() {
        return java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    BulkControlService bulkControlService(CoreClient core, ControlProfileService profiles, ShadowRepository shadows,
                                          SceneBulkRepository repository, CommandRepository commands, ControlFacade facade,
                                          RoleChecker roleChecker, java.util.concurrent.ExecutorService actionBackgroundExecutor, Clock clock) {
        return new BulkControlService(core, profiles, shadows, repository, commands, facade, roleChecker, actionBackgroundExecutor, clock);
    }

    @Bean
    SceneService sceneService(CoreClient core, ControlProfileService profiles, ShadowRepository shadows, InterlockService interlocks,
                              SceneBulkRepository repository, CommandRepository commands, ControlFacade facade, Clock clock) {
        return new SceneService(core, profiles, shadows, interlocks, repository, commands, facade, clock);
    }

    @Bean
    CommandQueryService commandQueryService(CommandRepository commands, CommandEventRepository timeline, ShadowRepository shadows,
                                            DeviceStateRepository deviceState, CommandEvents events, ControlProfileService profiles,
                                            DriverRegistry drivers, RoleChecker roleChecker, PlatformTransactionManager tx, Clock clock,
                                            EmergencyStopRegistry emergency) {
        return new CommandQueryService(commands, timeline, shadows, deviceState, events, profiles, drivers, roleChecker, tx, clock, emergency);
    }

    @Bean
    ActionRequestHandler actionRequestHandler(ControlFacade facade, CommandRepository commands, ExecutionRepository executions,
                                              CommandEvents events, ControlProfileService profiles, CoreClient core,
                                              PlatformTransactionManager tx, Clock clock, SceneService scenes) {
        return new ActionRequestHandler(facade, commands, executions, events, profiles, core, tx, clock, scenes);
    }

    // ───────────── 아웃박스(ADR-020) ─────────────

    @Bean
    RabbitOutboxDispatcher rabbitOutboxDispatcher(RabbitTemplate rabbitTemplate) {
        return new RabbitOutboxDispatcher(rabbitTemplate, Json.MAPPER);
    }

    @Bean
    CoreCallbackDispatcher coreCallbackDispatcher(ActionProperties properties) {
        return new CoreCallbackDispatcher(client(properties.coreUri()));
    }

    @Bean
    OutboxRelay outboxRelay(OutboxRepository repository, List<OutboxDispatcher> dispatchers, PlatformTransactionManager tx,
                            ActionProperties properties, Clock clock) {
        return new OutboxRelay(repository, dispatchers, tx, properties, clock);
    }
}
