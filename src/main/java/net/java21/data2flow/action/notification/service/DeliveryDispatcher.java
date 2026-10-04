package net.java21.data2flow.action.notification.service;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.channel.ChannelRegistry;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.NotificationRetryPolicy;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.NotificationDeliveryResult;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import net.java21.data2flow.contracts.notification.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 발송(채널 호출)과 재시도(OPS-06.03, RUL-03.05, BR-RUL-17·BR-OPS-06). 채널 종류로 분기하지 않는다(BR-OPS-32): 채널 정의를 읽고 SPI를
 * 키로 찾아 {@link ChannelMessage#adaptTo}로 맞춘 메시지를 보낸다. 웹 알림(WEB)은 채널 호출 없이 SENT다.
 *
 * <ol>
 *   <li>잠금 트랜잭션: 보낼 때가 된 행만, 호출 횟수를 올리고 다음 시각을 {@code sendLease} 뒤로 미룬다(다른 파드가 겹쳐 보내지 않음).</li>
 *   <li>트랜잭션 밖에서 채널을 부른다. 같은 멱등 키로 다시 불릴 수 있다(파드가 죽으면 lease 뒤 다시).</li>
 *   <li>결과 트랜잭션: 성공 → SENT + EVT-RUL-04 {@code notification.delivered}, 일시 실패 → RETRYING(30초·2분·10분·30분·1시간 또는
 *       Retry-After), 재시도 소진·영구 실패 → FAILED + {@code notification.failed}(core가 운영 알람을 만든다).</li>
 * </ol>
 */
public class DeliveryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DeliveryDispatcher.class);

    private final DeliveryRepository deliveries;
    private final NotificationCoreClient core;
    private final ChannelRegistry channels;
    private final NotificationRetryPolicy retry;
    private final OutboxWriter outbox;
    private final TransactionTemplate tx;
    private final NotificationProperties properties;
    private final ActionProperties actionProperties;
    private final MeterRegistry meters;
    private final Clock clock;

    public DeliveryDispatcher(DeliveryRepository deliveries, NotificationCoreClient core, ChannelRegistry channels,
                              NotificationRetryPolicy retry, OutboxWriter outbox, PlatformTransactionManager txManager,
                              NotificationProperties properties, ActionProperties actionProperties, MeterRegistry meters, Clock clock) {
        this.deliveries = deliveries;
        this.core = core;
        this.channels = channels;
        this.retry = retry;
        this.outbox = outbox;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.actionProperties = actionProperties;
        this.meters = meters;
        this.clock = clock;
    }

    /** 보낼 때가 된 발송(재시도·묶음 해제)을 보낸다. 처리 건수 */
    public int processDue() {
        List<UUID> due = tx.execute(s -> deliveries.lockDue(clock.instant(), properties.batch(), actionProperties.env()));
        if (due == null) {
            return 0;
        }
        due.forEach(this::send);
        return due.size();
    }

    public void sendAll(Collection<UUID> ids) {
        ids.forEach(this::send);
    }

    /** 한 건 보낸다. 보낼 상태·시각이 아니면 아무것도 하지 않는다 */
    public void send(UUID id) {
        Delivery d = tx.execute(s -> {
            Instant now = clock.instant();
            Optional<Delivery> locked = deliveries.lockSendable(id, now);
            locked.ifPresent(x -> deliveries.markAttempt(x.id(), x.attempt() + 1, now.plus(properties.sendLease())));
            return locked.orElse(null);
        });
        if (d == null) {
            return;
        }
        int attempt = d.attempt() + 1;
        SendResult result = call(d);
        meters.counter("data2flow_action_notifications_total", "channel", d.channelType(), "result", result.outcome().name()).increment();
        tx.executeWithoutResult(s -> complete(d, attempt, result));
    }

    private SendResult call(Delivery d) {
        if (d.web()) {
            return SendResult.success(null);   // 웹 알림 센터·토스트: core가 EVT-RUL-04로 보여 준다
        }
        Optional<ChannelDefinition> def = core.channel(d.channelId());
        if (def.isEmpty() || !def.get().enabled()) {
            return SendResult.permanentFailure("CHANNEL_NOT_FOUND: 채널이 없거나 꺼져 있습니다");
        }
        Optional<NotificationChannel> impl = channels.find(def.get().type());
        if (impl.isEmpty() || !impl.get().available()) {
            return SendResult.permanentFailure("CHANNEL_UNAVAILABLE: " + def.get().type() + " 채널 구현을 쓸 수 없습니다");
        }
        if (d.payload() == null || d.payload().address() == null) {
            return SendResult.permanentFailure("NO_ADDRESS: 받을 주소가 없습니다");
        }
        ChannelMessage message = d.payload().toMessage(d.idempotencyKey()).adaptTo(impl.get().capabilities());
        try {
            return impl.get().send(def.get().settings(), message);
        } catch (RuntimeException e) {
            // 계약 위반(예외)도 일시 실패로 다룬다
            log.warn("채널 예외 type={} delivery={}: {}", d.channelType(), d.id(), e.getClass().getSimpleName());
            return SendResult.transientFailure("채널 예외: " + e.getClass().getSimpleName(), null);
        }
    }

    private void complete(Delivery d, int attempt, SendResult result) {
        Instant now = clock.instant();
        switch (result.outcome()) {
            case SUCCESS -> {
                deliveries.markSent(d.id(), now, result.externalMessageId());
                event(d, DeliveryStatus.SENT, attempt, null, now);
            }
            case TRANSIENT_FAILURE -> {
                Optional<Duration> wait = retry.next(attempt, true, result.retryAfter());
                if (wait.isPresent()) {
                    deliveries.markRetry(d.id(), now.plus(wait.get()), result.error());
                } else {
                    deliveries.markFailed(d.id(), result.error());
                    event(d, DeliveryStatus.FAILED, attempt, result.error(), now);
                }
            }
            case PERMANENT_FAILURE -> {
                deliveries.markFailed(d.id(), result.error());
                event(d, DeliveryStatus.FAILED, attempt, result.error(), now);
            }
        }
    }

    /** EVT-RUL-04 */
    void event(Delivery d, DeliveryStatus status, int attempts, String error, Instant at) {
        EventType type = status == DeliveryStatus.SENT ? EventType.NOTIFICATION_DELIVERED : EventType.NOTIFICATION_FAILED;
        outbox.event(type, d.organizationId(), new NotificationDeliveryResult(d.id(), d.alarmId(), d.web() ? null : d.channelId(),
                d.channelType(), d.recipientKey(), status, attempts, error, at), "notification:" + d.id() + ":" + status.name());
    }
}
