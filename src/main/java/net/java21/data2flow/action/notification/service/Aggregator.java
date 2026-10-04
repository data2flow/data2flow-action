package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.DigestRule;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.action.notification.domain.NotificationTexts;
import net.java21.data2flow.action.notification.repository.AggregateRepository;
import net.java21.data2flow.action.notification.repository.AggregateRepository.Aggregate;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 묶음 창 닫기(BR-RUL-15·BR-OPS-07, OPS-06.05 방해 금지 끝). 창이 끝나면 기다리던 발송이 1건이면 그대로 보내고, 2건 이상이면 대기 행을
 * DIGESTED로 바꾸고 요약 1건("같은 알람 N건, 대표: …")을 만든다. 요약과 묶인 발송은 같은 {@code aggregate_id}로 이어진다(이력에 연결).
 */
public class Aggregator {

    private final AggregateRepository aggregates;
    private final DeliveryRepository deliveries;
    private final DeliveryDispatcher dispatcher;
    private final TransactionTemplate tx;
    private final NotificationProperties properties;
    private final ActionProperties actionProperties;
    private final Clock clock;

    public Aggregator(AggregateRepository aggregates, DeliveryRepository deliveries, DeliveryDispatcher dispatcher,
                      PlatformTransactionManager txManager, NotificationProperties properties, ActionProperties actionProperties, Clock clock) {
        this.aggregates = aggregates;
        this.deliveries = deliveries;
        this.dispatcher = dispatcher;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.actionProperties = actionProperties;
        this.clock = clock;
    }

    /** 끝난 창을 닫고 보낼 것을 보낸다. 닫은 창 수 */
    public int flushDue() {
        List<UUID> send = new ArrayList<>();
        Integer n = tx.execute(s -> {
            Instant now = clock.instant();
            List<Aggregate> due = aggregates.lockDue(now, properties.batch(), actionProperties.env());
            for (Aggregate a : due) {
                send.addAll(flush(a, now));
            }
            return due.size();
        });
        dispatcher.sendAll(send);
        return n == null ? 0 : n;
    }

    private List<UUID> flush(Aggregate a, Instant now) {
        List<Delivery> held = deliveries.lockHeld(a.organizationId(), a.id());
        DigestRule.Flush action = DigestRule.flush(held.size());
        if (action == DigestRule.Flush.NONE) {
            aggregates.flush(a.organizationId(), a.id(), null);
            return List.of();
        }
        if (action == DigestRule.Flush.SINGLE) {
            deliveries.release(a.organizationId(), held.get(0).id(), now);
            aggregates.flush(a.organizationId(), a.id(), held.get(0).id());
            return List.of(held.get(0).id());
        }
        Delivery first = held.get(0);
        MessagePayload p = first.payload();
        String locale = p == null ? properties.defaultLocale() : p.locale();
        String body = NotificationTexts.text("digest", locale, held.size(), p == null ? "" : p.title());
        if (a.sentCount() > 0) {
            body = body + " " + NotificationTexts.text("digestEarlier", locale, a.sentCount());
        }
        MessagePayload summary = new MessagePayload(p == null ? null : p.address(), p == null ? null : p.title(), body,
                p == null ? null : p.link(), List.of(), locale, p == null ? null : p.severity(), null);
        UUID id = UUID.randomUUID();
        Delivery d = new Delivery(id, a.organizationId(), ActionIdempotencyKeys.of("digest", Long.toString(a.id())), a.channelId(),
                a.channelType(), first.sourceType(), first.sourceId(), first.alarmId(), a.id(), a.recipientKey(), first.userId(),
                first.requestKey(), first.event(), first.severity(), null, summary, held.size(), DeliveryStatus.PENDING, null, 0, now, null,
                null, now, null);
        deliveries.markDigested(a.organizationId(), a.id());
        deliveries.insert(d, actionProperties.env());
        aggregates.flush(a.organizationId(), a.id(), id);
        return List.of(id);
    }
}
