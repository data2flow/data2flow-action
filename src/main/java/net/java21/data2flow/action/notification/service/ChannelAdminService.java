package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.notification.NotificationErrorCode;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.channel.ChannelRegistry;
import net.java21.data2flow.action.notification.channel.WebhookRegistrar;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.action.notification.domain.NotificationTexts;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.notification.repository.DeliveryRepository.DeliveryQuery;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.notification.ChannelCapabilities;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import net.java21.data2flow.contracts.notification.LinkRequest;
import net.java21.data2flow.contracts.notification.LinkResult;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import net.java21.data2flow.contracts.notification.SendResult;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.web.CursorListApiResponse;
import net.java21.data2flow.contracts.web.CursorParams;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 채널 운영(OPS-06.01·06.03·06.06, API-OPS-31·33·34·97, API-RUL-27·30): 채널 유형 목록, 테스트 발송, 웹훅 등록, 계정 연결 딥링크,
 * 시스템 알림 직접 발송, 발송 이력(커서), 다시 보내기. 채널 종류로 분기하지 않는다(SPI·선택 기능 인터페이스로만).
 */
public class ChannelAdminService {

    private final ChannelRegistry channels;
    private final NotificationCoreClient core;
    private final DeliveryRepository deliveries;
    private final DeliveryDispatcher dispatcher;
    private final TransactionTemplate tx;
    private final NotificationProperties properties;
    private final ActionProperties actionProperties;
    private final Clock clock;

    public ChannelAdminService(ChannelRegistry channels, NotificationCoreClient core, DeliveryRepository deliveries,
                               DeliveryDispatcher dispatcher, PlatformTransactionManager txManager, NotificationProperties properties,
                               ActionProperties actionProperties, Clock clock) {
        this.channels = channels;
        this.core = core;
        this.deliveries = deliveries;
        this.dispatcher = dispatcher;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.actionProperties = actionProperties;
        this.clock = clock;
    }

    /** API-OPS-34 채널 유형(설정 스키마로 화면 폼 자동 생성) */
    public record ChannelType(String key, boolean available, JsonNode configSchema, ChannelCapabilities capabilities) {
    }

    public List<ChannelType> types() {
        return channels.all().stream().sorted(Comparator.comparing(NotificationChannel::key))
                .map(c -> new ChannelType(c.key(), c.available(), c.configSchema(), c.capabilities())).toList();
    }

    /** API-OPS-31 테스트 발송 요청(저장 전이면 정의 전체, 저장 뒤면 channelId + 바뀐 값) */
    public record TestRequest(String channelId, String type, JsonNode config, Map<String, String> secrets, String chatId) {
    }

    /** 결과 {ok, latencyMs, providerResponse} */
    public record TestResult(boolean ok, long latencyMs, String providerResponse) {
    }

    public TestResult test(long organizationId, TestRequest req) {
        ChannelDefinition saved = req.channelId() == null ? null : core.channel(Long.parseLong(req.channelId()))
                .filter(c -> c.organizationId() == organizationId)
                .orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_NOT_FOUND));
        String type = req.type() != null ? req.type() : saved == null ? null : saved.type();
        NotificationChannel impl = channels.find(type).orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_TYPE_UNKNOWN));
        Map<String, Secret> secrets = new LinkedHashMap<>(saved == null ? Map.of() : saved.secrets());
        if (req.secrets() != null) {
            req.secrets().forEach((k, v) -> {
                if (v != null && !v.isBlank() && !Secret.MASK.equals(v)) {
                    secrets.put(k, Secret.of(v));
                }
            });
        }
        JsonNode config = req.config() != null ? req.config() : saved == null ? null : saved.config();
        ChannelDefinition def = new ChannelDefinition(saved == null ? 0 : saved.channelId(), organizationId, "test", impl.key(), config,
                secrets, 20, 60, true);
        String address = req.chatId() != null ? req.chatId() : def.defaultAddresses().stream().findFirst().orElseThrow(() ->
                new BusinessException(CommonErrorCode.INVALID_REQUEST, List.of(new FieldErrorDetail("chatId", "REQUIRED", "보낼 대화방이 없습니다"))));
        if (!impl.available()) {
            throw new BusinessException(NotificationErrorCode.CHANNEL_TEST_FAILED, "CHANNEL_UNAVAILABLE");
        }
        ChannelMessage m = new ChannelMessage(ActionIdempotencyKeys.of("test", UUID.randomUUID().toString()), address, "data2flow",
                NotificationTexts.text("test", properties.defaultLocale()), AlarmSeverity.INFO, null, List.of(), properties.defaultLocale(), null);
        long started = System.nanoTime();
        SendResult r = impl.send(new ChannelDefinition(def.channelId(), organizationId, def.name(), def.type(), def.config(), def.secrets(),
                20, 60, true).settings(), m.adaptTo(impl.capabilities()));
        long ms = (System.nanoTime() - started) / 1_000_000;
        if (r.outcome() != SendResult.Outcome.SUCCESS) {
            throw new BusinessException(NotificationErrorCode.CHANNEL_TEST_FAILED, r.error());
        }
        return new TestResult(true, ms, r.externalMessageId());
    }

    /** OPS-06.01 저장 때 콜백 주소 등록(텔레그램 setWebhook). 이 기능이 없는 채널은 409 */
    public Map<String, Object> registerWebhook(long organizationId, long channelId) {
        ChannelDefinition def = channel(organizationId, channelId);
        NotificationChannel impl = channels.find(def.type()).orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_TYPE_UNKNOWN));
        if (!(impl instanceof WebhookRegistrar registrar)) {
            throw new BusinessException(NotificationErrorCode.CHANNEL_WEBHOOK_UNSUPPORTED);
        }
        String error = registrar.registerWebhook(def.settings(), properties.telegram().webhookUrl());
        if (error != null) {
            throw new BusinessException(NotificationErrorCode.CHANNEL_TEST_FAILED, error);
        }
        return Map.of("registered", true);
    }

    /** API-RUL-30 계정 연결 딥링크(코드는 core가 만든다) */
    public LinkResult link(long organizationId, String type, long userId, String code, Instant expiresAt) {
        NotificationChannel impl = channels.find(type).orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_TYPE_UNKNOWN));
        ChannelDefinition def = core.channels(organizationId, impl.key()).stream().findFirst()
                .orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_NOT_CONFIGURED));
        return impl.link(def.settings(), new LinkRequest(userId, code, expiresAt));
    }

    /** API-OPS-97 시스템 알림 직접 발송 요청 */
    public record SystemNotification(String idempotencyKey, List<String> channelIds, String severity, String subject, String body,
                                     List<String> links, String sourceType, String sourceId) {
    }

    public String systemNotify(long organizationId, SystemNotification req) {
        if (req.idempotencyKey() == null || !ActionIdempotencyKeys.isValid(req.idempotencyKey()) || req.channelIds() == null
                || req.channelIds().isEmpty() || req.body() == null) {
            throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("idempotencyKey·channelIds·body", "REQUIRED", "필수입니다")));
        }
        String requestKey = ActionIdempotencyKeys.of("system", Long.toString(organizationId), req.idempotencyKey());
        AlarmSeverity severity = parseSeverity(req.severity());
        String link = req.links() == null || req.links().isEmpty() ? null : req.links().get(0);
        String sourceType = req.sourceType() == null ? "SYSTEM" : req.sourceType().toUpperCase(java.util.Locale.ROOT);
        if (!List.of("ALARM", "FLOW", "REPORT", "SYSTEM", "TEST").contains(sourceType)) {
            sourceType = "SYSTEM";
        }
        String st = sourceType;
        List<UUID> send = tx.execute(s -> {
            List<UUID> ids = new ArrayList<>();
            Instant now = clock.instant();
            for (String chId : req.channelIds()) {
                ChannelDefinition def = channel(organizationId, Long.parseLong(chId));
                for (String address : def.defaultAddresses()) {
                    String rk = "CHANNEL_DEFAULT@" + address;
                    UUID id = UUID.randomUUID();
                    MessagePayload p = new MessagePayload(address, req.subject(), req.body(), link, List.of(), properties.defaultLocale(),
                            severity, null);
                    Delivery d = new Delivery(id, organizationId, ActionIdempotencyKeys.notificationDelivery(requestKey, "system", rk, def.type()),
                            def.channelId(), def.type(), st, req.sourceId(), null, null, rk, null, requestKey, "system",
                            severity == null ? null : severity.name(), null, p, 1, DeliveryStatus.PENDING, null, 0, now, null, null, now, null);
                    if (deliveries.insert(d, actionProperties.env())) {
                        ids.add(id);
                    }
                }
            }
            return ids;
        });
        dispatcher.sendAll(send == null ? List.of() : send);
        return requestKey;
    }

    /** API-RUL-27 발송 이력 항목 */
    public record DeliveryView(String deliveryId, String alarmId, String channel, String channelId, String recipient, String status,
                               String skipReason, int attempts, String lastError, Instant sentAt, Instant createdAt, int digestCount,
                               Integer stepNo) {
        static DeliveryView of(Delivery d) {
            return new DeliveryView(d.id().toString(), d.alarmId() == null ? null : d.alarmId().toString(), d.channelType(),
                    d.web() ? null : Long.toString(d.channelId()), d.recipientKey(), d.status().name(), d.skipReason(), d.attempt(),
                    d.lastError(), d.sentAt(), d.createdAt(), d.digestCount(), d.stepNo());
        }
    }

    public CursorListApiResponse<DeliveryView> list(DeliveryQuery q, String cursor, Integer size) {
        CursorParams params = CursorParams.of(cursor, size);
        Instant at = null;
        UUID id = null;
        if (params.cursor() != null) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(params.cursor()), StandardCharsets.UTF_8).split("\\|", 2);
                at = Instant.parse(parts[0]);
                id = UUID.fromString(parts[1]);
            } catch (RuntimeException e) {
                throw new BusinessException(CommonErrorCode.INVALID_REQUEST,
                        List.of(new FieldErrorDetail("cursor", "INVALID", "올바르지 않은 커서입니다")));
            }
        }
        List<Delivery> rows = deliveries.page(q, at, id, params.size() + 1);
        boolean more = rows.size() > params.size();
        List<Delivery> page = more ? rows.subList(0, params.size()) : rows;
        String next = null;
        if (more) {
            Delivery last = page.get(page.size() - 1);
            next = Base64.getUrlEncoder().withoutPadding().encodeToString((last.createdAt() + "|" + last.id()).getBytes(StandardCharsets.UTF_8));
        }
        return CursorListApiResponse.of(params.size(), page.stream().map(DeliveryView::of).toList(), next);
    }

    /** API-OPS-33 실패·건너뛴 발송 다시 보내기. 성공 건은 409 */
    public String resend(long organizationId, UUID deliveryId) {
        Delivery d = deliveries.findByIdAndOrganizationId(deliveryId, organizationId)
                .orElseThrow(() -> new BusinessException(NotificationErrorCode.DELIVERY_NOT_FOUND));
        if (d.status() != DeliveryStatus.FAILED && d.status() != DeliveryStatus.SKIPPED || d.payload() == null) {
            throw new BusinessException(NotificationErrorCode.DELIVERY_NOT_RESENDABLE);
        }
        UUID id = UUID.randomUUID();
        Instant now = clock.instant();
        Delivery copy = new Delivery(id, organizationId, ActionIdempotencyKeys.of("resend", d.id().toString(), now.toString()), d.channelId(),
                d.channelType(), d.sourceType(), d.sourceId(), d.alarmId(), null, d.recipientKey(), d.userId(), d.requestKey(), d.event(),
                d.severity(), d.stepNo(), d.payload(), d.digestCount(), DeliveryStatus.PENDING, null, 0, now, null, null, now, null);
        tx.executeWithoutResult(s -> deliveries.insert(copy, actionProperties.env()));
        dispatcher.send(id);
        return id.toString();
    }

    private ChannelDefinition channel(long organizationId, long channelId) {
        return core.channel(channelId).filter(c -> c.organizationId() == organizationId)
                .orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_NOT_FOUND));
    }

    private static AlarmSeverity parseSeverity(String s) {
        try {
            return s == null ? null : AlarmSeverity.valueOf(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
