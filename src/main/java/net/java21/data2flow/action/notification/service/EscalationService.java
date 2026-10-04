package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.domain.AlarmInfo;
import net.java21.data2flow.action.notification.domain.PolicyDefinition;
import net.java21.data2flow.action.notification.repository.EscalationRepository;
import net.java21.data2flow.action.notification.repository.EscalationRepository.Escalation;
import net.java21.data2flow.action.outbox.OutboxRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.alarm.AlarmStatus;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.notification.NotificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * 에스컬레이션(RUL-03.03, BR-RUL-16, TC-RUL-074~077): 단계별 대기 시간 안에 확인되지 않으면 다음 단계 수신자에게 보내고, 확인·해제되면
 * 남은 단계를 취소한다(최대 3단계). 취소는 {@code alarm.acked}·{@code alarm.cleared}(EVT-RUL-02)를 받아 바로 하고, 이벤트를 놓쳐도 기한에
 * 알람 상태(API-RUL-41)를 다시 확인한다. 보낸 단계는 알람 이력에 ESCALATED로 남긴다(API-RUL-50, 아웃박스 CALLBACK).
 */
public class EscalationService {

    private static final Logger log = LoggerFactory.getLogger(EscalationService.class);

    private final EscalationRepository escalations;
    private final NotificationCoreClient core;
    private final NotificationService notifications;
    private final DeliveryDispatcher dispatcher;
    private final OutboxRepository outbox;
    private final TransactionTemplate tx;
    private final NotificationProperties properties;
    private final ActionProperties actionProperties;
    private final Clock clock;

    public EscalationService(EscalationRepository escalations, NotificationCoreClient core, NotificationService notifications,
                             DeliveryDispatcher dispatcher, OutboxRepository outbox, PlatformTransactionManager txManager,
                             NotificationProperties properties, ActionProperties actionProperties, Clock clock) {
        this.escalations = escalations;
        this.core = core;
        this.notifications = notifications;
        this.dispatcher = dispatcher;
        this.outbox = outbox;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.actionProperties = actionProperties;
        this.clock = clock;
    }

    /** 확인·해제: 남은 단계 취소 */
    public int cancel(long organizationId, long alarmId) {
        Integer n = tx.execute(s -> escalations.cancel(organizationId, alarmId, clock.instant()));
        return n == null ? 0 : n;
    }

    /** 기한이 된 단계를 보낸다. 처리 건수 */
    public int processDue() {
        List<UUID> send = new ArrayList<>();
        Integer n = tx.execute(s -> {
            Instant now = clock.instant();
            List<Escalation> due = escalations.lockDue(now, properties.batch(), actionProperties.env());
            for (Escalation e : due) {
                try {
                    send.addAll(step(e, now));
                } catch (RuntimeException ex) {
                    log.warn("에스컬레이션 처리 실패(다음 주기에 다시) alarm={}: {}", e.alarmId(), ex.toString());
                    throw ex;
                }
            }
            return due.size();
        });
        dispatcher.sendAll(send);
        return n == null ? 0 : n;
    }

    private List<UUID> step(Escalation e, Instant now) {
        Optional<AlarmInfo> alarm = core.alarm(e.alarmId());
        if (alarm.isEmpty() || alarm.get().status() != AlarmStatus.ACTIVE) {
            escalations.finish(e.organizationId(), e.id(), "CANCELLED", now);
            return List.of();
        }
        Optional<PolicyDefinition> policy = core.policy(e.policyId());
        Optional<PolicyDefinition.Step> step = policy.flatMap(p -> p.step(e.nextStep()));
        if (step.isEmpty()) {
            escalations.finish(e.organizationId(), e.id(), "DONE", now);
            return List.of();
        }
        NotificationRequest original = Json.read(e.request(), NotificationRequest.class);
        List<NotificationRecipient> recipients = NotificationService.stepRecipients(policy.get(), step.get());
        NotificationRequest stepRequest = new NotificationRequest(original.alarmId(), NotificationEvents.ALARM_ESCALATED,
                original.eventSeq(), original.severity(), null, recipients, original.templateKeys(), original.variables(), 0, null,
                new NotificationRequest.Escalation(e.nextStep()), original.link(), original.virtual());
        String requestKey = ActionIdempotencyKeys.notifyRequest(e.alarmId(), NotificationEvents.ALARM_RAISED,
                original.eventSeq() == null ? 0 : original.eventSeq(), e.nextStep());
        List<UUID> ids = notifications.deliver(e.organizationId(), requestKey, stepRequest, "ALARM", Long.toString(e.alarmId()), false);
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "ESCALATED");
        event.put("at", now.toString());
        event.put("data", Map.of("stepNo", e.nextStep(), "policyId", Long.toString(e.policyId()),
                "recipients", recipients.stream().map(NotificationRecipient::recipientKey).distinct().toList()));
        outbox.insert(e.organizationId(), ActionIdempotencyKeys.of("escalated", Long.toString(e.alarmId()), Long.toString(e.policyId()),
                        Integer.toString(e.nextStep())), OutboxWriter.KIND_CALLBACK, OutboxWriter.CORE_CALLBACK,
                "/internal/core/alarms/" + e.alarmId() + "/events", Json.write(event), now, actionProperties.env());
        Optional<PolicyDefinition.Step> next = policy.get().step(e.nextStep() + 1);
        if (next.isPresent()) {
            escalations.advance(e.organizationId(), e.id(), e.nextStep() + 1, now.plus(Duration.ofMinutes(step.get().waitMinutes())), now);
        } else {
            escalations.finish(e.organizationId(), e.id(), "DONE", now);
        }
        return ids;
    }
}
