package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.output.domain.DeliveryResult;
import net.java21.data2flow.action.output.domain.OutboundMessage;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.action.output.domain.RetryPolicy;
import net.java21.data2flow.action.output.repository.OutputDeliveryRepository;
import net.java21.data2flow.action.output.repository.OutputDeliveryRepository.DeliveryRow;
import net.java21.data2flow.action.output.transport.MqttOutputPublisher;
import net.java21.data2flow.action.output.transport.WebhookOutputSender;
import net.java21.data2flow.contracts.output.OutputConnectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 출력 연결 발송 작업(DSC-04.01, BR-DSC-19, AT-DSC-10.2).
 * <ul>
 *   <li>연결마다 리스를 잡은 파드 하나가 맨 앞 대기 행부터 순서대로 보낸다(앞 행이 재시도 대기 중이면 뒤 행도 기다린다 → 밀린 메시지가 순서대로)</li>
 *   <li>MQTT는 한 건씩, Webhook은 {@code batchSize}(기본 100)까지 JSON 배열 하나로. 덜 찼으면 첫 행이 쌓인 뒤 {@code batchWaitMs}(기본 1초)까지 모은다</li>
 *   <li>실패하면 1초부터 2배씩 최대 5분 간격으로 다시 시도하고, 재시도 시작 24시간이 지나면 FAILED(실패 보관함)로 옮겨 뒤 행을 막지 않는다</li>
 *   <li>연결이 지워졌으면 대기 행을 FAILED(CONNECTION_REMOVED)로, 꺼졌으면 켜질 때까지 둔다</li>
 * </ul>
 */
public class OutputDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(OutputDeliveryService.class);
    /** 한 연결을 한 번에 연속 처리하는 최대 묶음 수(다른 연결도 돌게) */
    static final int MAX_ROUNDS = 20;

    private final OutputConnectionRegistry registry;
    private final OutputDeliveryRepository deliveries;
    private final MqttOutputPublisher mqtt;
    private final WebhookOutputSender webhook;
    private final OutputStats stats;
    private final RetryPolicy retry;
    private final Clock clock;
    private final String env;
    private final String holder;
    private final Duration lease;
    private final int batch;

    public OutputDeliveryService(OutputConnectionRegistry registry, OutputDeliveryRepository deliveries, MqttOutputPublisher mqtt,
                                 WebhookOutputSender webhook, OutputStats stats, RetryPolicy retry, Clock clock, String env, String holder,
                                 Duration lease, int batch) {
        this.registry = registry;
        this.deliveries = deliveries;
        this.mqtt = mqtt;
        this.webhook = webhook;
        this.stats = stats;
        this.retry = retry;
        this.clock = clock;
        this.env = env;
        this.holder = holder;
        this.lease = lease;
        this.batch = batch;
    }

    /** 대기 행이 있는 모든 연결을 한 번 돈다. 보낸 행 수 */
    public int deliverDue() {
        registry.refreshIfStale();
        int sent = 0;
        for (long[] p : deliveries.listPendingOutputs(env)) {
            try {
                sent += deliver(p[1], p[0]);
            } catch (RuntimeException e) {
                log.warn("출력 연결 {} 발송 작업 실패(다음 주기에 다시): {}", p[0], e.toString());
            }
        }
        return sent;
    }

    int deliver(long organizationId, long outputId) {
        Optional<OutputConnection> found = registry.find(outputId);
        Instant now = clock.instant();
        if (found.isEmpty()) {
            if (registry.loaded()) {
                int n = deliveries.updateOrphaned(organizationId, outputId, env, now);
                if (n > 0) {
                    log.info("지워진 출력 연결 {}의 대기 {}건을 실패 보관함으로 옮깁니다", outputId, n);
                }
            }
            return 0;
        }
        OutputConnection c = found.get();
        if (!c.enabled() || c.organizationId() != organizationId) {
            return 0;
        }
        if (!deliveries.acquireLease(organizationId, outputId, env, holder, now, now.plus(lease))) {
            return 0; // 다른 파드가 보내는 중
        }
        int sent = 0;
        for (int round = 0; round < MAX_ROUNDS; round++) {
            int n = deliverOnce(c);
            if (n <= 0) {
                break;
            }
            sent += n;
        }
        return sent;
    }

    /** 맨 앞 묶음을 한 번 보낸다. 보낸 수, 보낼 것이 없거나 기다리면 0, 실패하면 -1 */
    int deliverOnce(OutputConnection c) {
        boolean isWebhook = c.type() == OutputConnectionType.WEBHOOK;
        int limit = isWebhook ? Math.max(1, Math.min(c.target().path("batchSize").asInt(100), 500)) : batch;
        limit = Math.min(limit, batch);
        List<DeliveryRow> head = deliveries.listHead(c.organizationId(), c.id(), env, limit);
        Instant now = clock.instant();
        if (head.isEmpty() || head.getFirst().nextAttemptAt().isAfter(now)) {
            return 0;
        }
        if (isWebhook) {
            long waitMs = Math.max(0, Math.min(c.target().path("batchWaitMs").asLong(1000), 10_000));
            if (head.size() < limit && head.getFirst().attempts() == 0 && head.getFirst().createdAt().plusMillis(waitMs).isAfter(now)) {
                return 0; // 배치를 더 모은다
            }
            List<OutboundMessage> items = head.stream().map(r -> new OutboundMessage(r.part(), r.topic(), r.body())).toList();
            DeliveryResult result = webhook.send(c, items);
            return result.ok() ? markSent(c, head) : markFailure(c, head, result);
        }
        List<DeliveryRow> done = new ArrayList<>();
        for (DeliveryRow r : head) {
            if (r.nextAttemptAt().isAfter(clock.instant())) {
                break;
            }
            DeliveryResult result = mqtt.publish(c, new OutboundMessage(r.part(), r.topic(), r.body()));
            if (!result.ok()) {
                markSent(c, done);
                markFailure(c, List.of(r), result);
                return done.isEmpty() ? -1 : done.size();
            }
            done.add(r);
        }
        return markSent(c, done);
    }

    private int markSent(OutputConnection c, List<DeliveryRow> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        Instant now = clock.instant();
        deliveries.updateSent(c.organizationId(), rows.stream().map(DeliveryRow::id).toList(), now);
        long lag = rows.stream().mapToLong(r -> Math.max(0, Duration.between(r.measuredAt(), now).toMillis())).max().orElse(0);
        int retried = (int) rows.stream().filter(r -> r.attempts() > 0).count();
        stats.sent(c.id(), c.organizationId(), rows.size(), retried, lag);
        return rows.size();
    }

    private int markFailure(OutputConnection c, List<DeliveryRow> rows, DeliveryResult result) {
        Instant now = clock.instant();
        String kind = result.failureKind() == null ? null : result.failureKind().name();
        for (DeliveryRow r : rows) {
            if (retry.expired(r.retryStartedAt(), now)) {
                deliveries.updateFailed(c.organizationId(), r.id(), now, result.error(), kind);
            } else {
                deliveries.updateRetry(c.organizationId(), r.id(), now.plus(retry.delayAfter(r.attempts() + 1)), result.error(), kind);
            }
        }
        stats.failed(c.id(), c.organizationId(), rows.size());
        log.debug("출력 연결 {} 발송 실패 {}건: {} {}", c.id(), rows.size(), kind, result.error());
        return -1;
    }

    /** 실패 보관함 재전송(API-DSC-77). 다시 대기로 돌린 수 */
    public int replayFailed(long organizationId, long outputId, Instant from, Instant to) {
        return deliveries.updateReplay(organizationId, outputId, from, to, clock.instant());
    }

    /** 종료: 잡은 리스를 놓아 다른 파드가 바로 넘겨받게 한다 */
    public void releaseLeases() {
        deliveries.releaseLeases(env, holder);
    }
}
