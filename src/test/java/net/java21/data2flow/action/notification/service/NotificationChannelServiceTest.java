package net.java21.data2flow.action.notification.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.channel.ChannelRegistry;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.action.notification.domain.NotificationRetryPolicy;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.NotificationDeliveryResult;
import net.java21.data2flow.contracts.notification.CallbackCommand;
import net.java21.data2flow.contracts.notification.ChannelCapabilities;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.ChannelSettings;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import net.java21.data2flow.contracts.notification.InboundRequest;
import net.java21.data2flow.contracts.notification.LinkRequest;
import net.java21.data2flow.contracts.notification.LinkResult;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import net.java21.data2flow.contracts.notification.SendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 발송·재시도(OPS-06.03, BR-OPS-06): {@link DeliveryDispatcher}를 메모리 저장소·시험 채널·MutableClock으로 돌린다 */
class NotificationChannelServiceTest {

    private final MutableClock clock = MutableClock.atUtc("2026-10-03T00:00:00Z");
    private final Instant outageEnd;
    private final MemoryRepo repo = new MemoryRepo();
    private final OutboxWriter outbox = mock(OutboxWriter.class);
    private final NotificationCoreClient core = mock(NotificationCoreClient.class);
    private final List<Instant> calls = new ArrayList<>();
    private DeliveryDispatcher dispatcher;

    NotificationChannelServiceTest() {
        outageEnd = clock.instant().plus(Duration.ofMinutes(2));
    }

    private void setUp(Instant until) {
        when(core.channel(6L)).thenReturn(Optional.of(ChannelDefinition.from(Json.MAPPER.valueToTree(NotificationFixtures.fakeChannel(6))))
                .map(c -> new ChannelDefinition(c.channelId(), c.organizationId(), c.name(), "FLAKY", c.config(), c.secrets(), 60, 60, true)));
        NotificationChannel flaky = new Flaky(until);
        dispatcher = new DeliveryDispatcher(repo, core, new ChannelRegistry(List.of(flaky)),
                new NotificationRetryPolicy(NotificationProperties.SPEC_BACKOFFS), outbox, mock(PlatformTransactionManager.class),
                NotificationProperties.defaults(), actionProperties(),
                new SimpleMeterRegistry(), clock);
        repo.d = new Delivery(UUID.randomUUID(), 1, "k-1", 6, "FLAKY", "ALARM", "9001", 9001L, null, "USER:5", 5L, null, "alarm.raised",
                "MAJOR", null, new MessagePayload("555", "t", "b", null, List.of(), "ko", null, null), 1, DeliveryStatus.PENDING, null, 0,
                clock.instant(), null, null, clock.instant(), null);
    }

    private static ActionProperties actionProperties() {
        ActionProperties p = mock(ActionProperties.class);
        when(p.env()).thenReturn("test");
        return p;
    }

    /** 다음 시도 시각까지 시계를 돌리며 보낸다 */
    private void runUntilDone() {
        for (int i = 0; i < 10 && !repo.d.status().terminal(); i++) {
            if (repo.d.nextRetryAt() != null && repo.d.nextRetryAt().isAfter(clock.instant())) {
                clock.set(repo.d.nextRetryAt());
            }
            dispatcher.send(repo.d.id());
        }
    }

    @Test
    @DisplayName("[OPS-06.03][AT-OPS-12.1][TC-OPS-063] 채널 API 일시 장애(2분) → 30초 뒤 재시도 실패, 2분 뒤 재시도에서 성공, 이력에 시도 3회")
    void recoversOnThirdAttempt() {
        setUp(outageEnd);
        runUntilDone();
        assertThat(repo.d.status()).isEqualTo(DeliveryStatus.SENT);
        assertThat(repo.d.attempt()).isEqualTo(3);
        assertThat(calls).containsExactly(Instant.parse("2026-10-03T00:00:00Z"), Instant.parse("2026-10-03T00:00:30Z"),
                Instant.parse("2026-10-03T00:02:30Z"));
        verify(outbox).event(eq(EventType.NOTIFICATION_DELIVERED), eq(1L), any(NotificationDeliveryResult.class), anyString());
    }

    @Test
    @DisplayName("[OPS-06.03][AT-OPS-12.2][TC-OPS-064] 계속 실패 → 재시도 5회(30초·2분·10분·30분·1시간) 후 FAILED, notification.failed(운영 알람은 core)")
    void failsAfterFiveRetries() {
        setUp(Instant.MAX);
        runUntilDone();
        assertThat(repo.d.status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(repo.d.attempt()).isEqualTo(6);
        assertThat(calls).hasSize(6);
        assertThat(Duration.between(calls.get(0), calls.get(5))).isEqualTo(Duration.ofSeconds(30 + 120 + 600 + 1800 + 3600));
        verify(outbox).event(eq(EventType.NOTIFICATION_FAILED), eq(1L), any(NotificationDeliveryResult.class), anyString());
    }

    @Test
    @DisplayName("[OPS-06.03] 채널이 꺼져 있거나 없으면 영구 실패(재시도 없음)")
    void unavailableChannel() {
        setUp(Instant.MIN);
        when(core.channel(6L)).thenReturn(Optional.empty());
        dispatcher.send(repo.d.id());
        assertThat(repo.d.status()).isEqualTo(DeliveryStatus.FAILED);
        assertThat(repo.d.lastError()).startsWith("CHANNEL_NOT_FOUND");
        assertThat(calls).isEmpty();
    }

    /** until 전까지 503을 돌려주는 채널 */
    private final class Flaky implements NotificationChannel {
        private final Instant until;

        Flaky(Instant until) {
            this.until = until;
        }

        @Override
        public String key() {
            return "FLAKY";
        }

        @Override
        public JsonNode configSchema() {
            return Json.MAPPER.createObjectNode().put("type", "object");
        }

        @Override
        public ChannelCapabilities capabilities() {
            return new ChannelCapabilities(false, Set.of(), 1000, 60, 0);
        }

        @Override
        public SendResult send(ChannelSettings settings, ChannelMessage message) {
            calls.add(clock.instant());
            return clock.instant().isBefore(until) ? SendResult.transientFailure("503", null) : SendResult.success("m-1");
        }

        @Override
        public boolean verify(ChannelSettings settings, InboundRequest request) {
            return false;
        }

        @Override
        public Optional<CallbackCommand> handleCallback(ChannelSettings settings, InboundRequest request) {
            return Optional.empty();
        }

        @Override
        public LinkResult link(ChannelSettings settings, LinkRequest request) {
            return new LinkResult("x", request.expiresAt());
        }
    }

    /** 발송 한 건만 담는 메모리 저장소 */
    static final class MemoryRepo extends DeliveryRepository {
        Delivery d;

        MemoryRepo() {
            super(null);
        }

        private Delivery with(DeliveryStatus status, int attempt, Instant next, String error, String ext) {
            return new Delivery(d.id(), d.organizationId(), d.idempotencyKey(), d.channelId(), d.channelType(), d.sourceType(), d.sourceId(),
                    d.alarmId(), d.aggregateId(), d.recipientKey(), d.userId(), d.requestKey(), d.event(), d.severity(), d.stepNo(), d.payload(),
                    d.digestCount(), status, d.skipReason(), attempt, next, error, ext, d.createdAt(), d.sentAt());
        }

        @Override
        public Optional<Delivery> lockSendable(UUID id, Instant now) {
            boolean ok = (d.status() == DeliveryStatus.PENDING || d.status() == DeliveryStatus.RETRYING) && d.nextRetryAt() != null
                    && !d.nextRetryAt().isAfter(now);
            return ok ? Optional.of(d) : Optional.empty();
        }

        @Override
        public void markAttempt(UUID id, int attempt, Instant leaseUntil) {
            d = with(d.status(), attempt, leaseUntil, d.lastError(), null);
        }

        @Override
        public void markSent(UUID id, Instant at, String ext) {
            d = with(DeliveryStatus.SENT, d.attempt(), null, null, ext);
        }

        @Override
        public void markRetry(UUID id, Instant next, String error) {
            d = with(DeliveryStatus.RETRYING, d.attempt(), next, error, null);
        }

        @Override
        public void markFailed(UUID id, String error) {
            d = with(DeliveryStatus.FAILED, d.attempt(), null, error, null);
        }
    }
}
