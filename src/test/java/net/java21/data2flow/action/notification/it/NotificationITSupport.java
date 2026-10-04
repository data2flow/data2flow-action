package net.java21.data2flow.action.notification.it;

import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.action.notification.service.Aggregator;
import net.java21.data2flow.action.notification.service.DeliveryDispatcher;
import net.java21.data2flow.action.notification.service.EscalationService;
import net.java21.data2flow.action.notification.service.NotificationService;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.notification.NotificationRequest;
import net.java21.data2flow.contracts.test.notification.FakeNotificationChannel;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/** 알림 통합 시험 공통: 가짜 core 알림 API(NotificationFixtures)·텔레그램 대역(FakeTelegram)·가짜 채널(FakeNotificationChannel) */
abstract class NotificationITSupport extends IntegrationTestSupport {

    protected NotificationFixtures fx;
    @Autowired
    protected NotificationService notifications;
    @Autowired
    protected DeliveryDispatcher dispatcher;
    @Autowired
    protected Aggregator aggregator;
    @Autowired
    protected EscalationService escalations;
    @Autowired
    protected FakeNotificationChannel fake;

    @BeforeEach
    void installNotificationFixtures() {
        fx = new NotificationFixtures();
        fx.install(CORE);
        fx.alarms.put(NotificationFixtures.ALARM, NotificationFixtures.alarm(NotificationFixtures.ALARM, "MAJOR", "ACTIVE"));
        fx.users.put(5L, NotificationFixtures.user(5, "OPERATOR", "555"));
        fx.users.put(6L, NotificationFixtures.user(6, "OPERATOR", "666"));
        fx.users.put(7L, NotificationFixtures.user(7, "ADMIN", "777"));
        coreCaches().invalidateAll();
    }

    @Autowired
    private net.java21.data2flow.action.notification.service.NotificationCoreClient coreClient;

    protected net.java21.data2flow.action.notification.service.NotificationCoreClient coreCaches() {
        return coreClient;
    }

    /** 알람 알림 요청(core 정책 평가 결과 모양) */
    protected ActionRequest alarmRequest(long alarmId, String event, long seq, AlarmSeverity severity, Integer window,
                                         List<NotificationRecipient> recipients, Long policyId) {
        NotificationRequest n = new NotificationRequest(alarmId, event, seq, severity, policyId, recipients, Map.of(), Map.of(), window, null,
                null, null, null);
        return ActionRequest.notify(NotificationFixtures.ORG, ActionIdempotencyKeys.notifyRequest(alarmId, event, seq, null),
                CommandSource.system(), null, n, clock);
    }

    protected void send(ActionRequest req) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        MessageHeaders.of(req).forEach(props::setHeader);
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, req.routingKey(), new Message(CODEC.write(req), props));
    }

    protected long deliveries(String where) {
        return count("SELECT count(*) FROM data2flow_action.notification_deliveries WHERE " + where);
    }

    protected long outbox(String routingKey) {
        return count("SELECT count(*) FROM data2flow_action.outboxes WHERE routing_key = '" + routingKey + "'");
    }

    /** 다음 재시도 시각까지 시계를 돌리고 재시도 작업을 한 번 돌린다 */
    protected void advanceToNextRetryAndRun() {
        java.time.Instant next = jdbc.sql("SELECT min(next_retry_at) FROM data2flow_action.notification_deliveries WHERE status IN ('PENDING','RETRYING')")
                .query(java.time.OffsetDateTime.class).list().stream().filter(java.util.Objects::nonNull)
                .map(java.time.OffsetDateTime::toInstant).findFirst().orElse(null);
        if (next != null && next.isAfter(clock.instant())) {
            clock.set(next);
        }
        dispatcher.processDue();
    }
}
