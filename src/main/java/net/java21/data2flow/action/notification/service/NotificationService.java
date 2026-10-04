package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.action.notification.domain.AlarmInfo;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.DigestRule;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.action.notification.domain.NotificationTexts;
import net.java21.data2flow.action.notification.domain.PolicyDefinition;
import net.java21.data2flow.action.notification.domain.RenotifyPolicy;
import net.java21.data2flow.action.notification.domain.SilenceMatcher;
import net.java21.data2flow.action.notification.domain.TemplateRenderer;
import net.java21.data2flow.action.notification.repository.AggregateRepository;
import net.java21.data2flow.action.notification.repository.AggregateRepository.Aggregate;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.notification.repository.EscalationRepository;
import net.java21.data2flow.action.notification.service.RecipientResolver.Target;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.alarm.AlarmStatus;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.ActionKind;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.notification.CallbackAction;
import net.java21.data2flow.contracts.notification.ChannelButton;
import net.java21.data2flow.contracts.notification.DeliverySkipReasons;
import net.java21.data2flow.contracts.notification.DeliveryStatus;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.notification.NotificationRequest;
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
 * 알림 요청 처리(EVT-RUL-03 {@code action.notifications}, RUL-03·OPS-06). 채널과 무관한 공통 계층의 입구다(BR-OPS-32).
 *
 * <p>순서: 요청 멱등(executed_actions NOTIFY, BR-RUL-17) → 알람·정책 읽기 → 수신자 계산({@link RecipientResolver}) → 수신자마다
 * 발송 멱등 키 → 건너뛰기 판정(연결 없음·권한 없음·플래핑·억제·가상 끄기·무음·최소 심각도·재알림 간격) → 템플릿 → 방해 금지 대기·
 * 묶음·한도 판단 → 발송 행 기록(같은 트랜잭션) → 커밋 뒤 바로 보낼 행만 보낸다. 실패한 호출은 재시도 작업이 다시 보낸다.
 *
 * <p>묶음(조정한 의미, ADR 예정): ① 요청·정책 묶기 창이 있으면 창이 끝날 때까지 모두 기다렸다가 1건이면 그대로, 2건 이상이면 요약 1건
 * (BR-RUL-15). ② 창이 없으면 채널 기본 묶음 창(60초) 안의 같은 묶음 키 알림 중 앞 4건은 바로 보내고 5번째부터 모아 창 끝에 요약 1건
 * (BR-OPS-07). 채널 분당 한도를 넘는 알림도 요약으로 모으고 버리지 않는다(OPS-06.04). 이렇게 해야 바로 보내는 알림이 30초 안에 나간다
 * (NFR-01.11).
 */
public class NotificationService {

    public static final String CONSUMER = "action.notifications";
    public static final String KIND_EXPLICIT = "EXPLICIT";
    public static final String KIND_CHANNEL = "CHANNEL";
    public static final String KIND_DND = "DND";

    private final NotificationCoreClient core;
    private final RecipientResolver resolver;
    private final DeliveryRepository deliveries;
    private final AggregateRepository aggregates;
    private final EscalationRepository escalations;
    private final ExecutionRepository executions;
    private final DeliveryDispatcher dispatcher;
    private final TransactionTemplate tx;
    private final NotificationProperties properties;
    private final ActionProperties actionProperties;
    private final Clock clock;

    public NotificationService(NotificationCoreClient core, RecipientResolver resolver, DeliveryRepository deliveries,
                               AggregateRepository aggregates, EscalationRepository escalations, ExecutionRepository executions,
                               DeliveryDispatcher dispatcher, PlatformTransactionManager txManager, NotificationProperties properties,
                               ActionProperties actionProperties, Clock clock) {
        this.core = core;
        this.resolver = resolver;
        this.deliveries = deliveries;
        this.aggregates = aggregates;
        this.escalations = escalations;
        this.executions = executions;
        this.dispatcher = dispatcher;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.actionProperties = actionProperties;
        this.clock = clock;
    }

    /** 알림 요청 하나. 지금 보낸 발송 ID */
    public List<UUID> handle(ActionRequest req) {
        if (req.kind() != ActionKind.NOTIFY) {
            throw new MessageFormatException("알림 큐에 알림이 아닌 행동이 왔습니다: " + req.kind());
        }
        NotificationRequest n;
        try {
            n = req.notificationRequest();
        } catch (RuntimeException e) {
            throw new MessageFormatException("알림 요청 본문을 읽을 수 없습니다: " + e.getMessage());
        }
        tx.executeWithoutResult(s -> executions.markProcessed(CONSUMER, req.messageId().toString(), clock.instant()));
        if (executions.findExecuted(req.organizationId(), req.idempotencyKey()).isPresent()) {
            return List.of();   // 같은 요청은 한 번만(TC-RUL-081)
        }
        String sourceType = n.alarmId() != null ? "ALARM" : switch (req.source().type()) {
            case FLOW, RULE -> "FLOW";
            default -> "SYSTEM";
        };
        String sourceId = n.alarmId() != null ? n.alarmId().toString() : req.source().flowId();
        List<UUID> now = deliver(req.organizationId(), req.idempotencyKey(), n, sourceType, sourceId, true);
        dispatcher.sendAll(now);
        return now;
    }

    /**
     * 요청 하나를 발송 행으로 만든다(한 트랜잭션). 바로 보낼 발송 ID를 돌려준다(커밋 뒤 호출 쪽이 보낸다).
     *
     * @param scheduleEscalation 처음 알림이면 정책 단계로 에스컬레이션을 예약한다
     */
    public List<UUID> deliver(long organizationId, String requestKey, NotificationRequest n, String sourceType, String sourceId,
                              boolean scheduleEscalation) {
        Optional<AlarmInfo> alarm = n.alarmId() == null ? Optional.empty() : core.alarm(n.alarmId());
        Optional<PolicyDefinition> policy = n.policyId() == null ? Optional.empty() : core.policy(n.policyId());
        if (n.recipients().isEmpty() && policy.isEmpty()) {
            throw new MessageFormatException("수신자도 정책도 없는 알림입니다(policyId=" + n.policyId() + ")");
        }
        Instant now = clock.instant();
        boolean sendClear = RenotifyPolicy.sendClear(n.event(), policy.orElse(null));
        List<Target> targets = sendClear
                ? resolver.resolve(organizationId, RecipientResolver.requested(n, policy.orElse(null)), alarm.orElse(null), now)
                : List.of();
        List<UUID> sendNow = tx.execute(s -> {
            List<UUID> ids = new ArrayList<>();
            for (Target t : targets) {
                plan(organizationId, requestKey, n, sourceType, sourceId, alarm.orElse(null), policy.orElse(null), t, now)
                        .ifPresent(ids::add);
            }
            if (scheduleEscalation) {
                scheduleEscalation(organizationId, n, policy.orElse(null), now);
            }
            executions.recordExecuted(organizationId, requestKey, ActionKind.NOTIFY.name(), "n=" + targets.size(),
                    sendClear ? "PLANNED" : "NO_CLEAR_NOTICE", now);
            return ids;
        });
        return sendNow == null ? List.of() : sendNow;
    }

    private void scheduleEscalation(long organizationId, NotificationRequest n, PolicyDefinition policy, Instant now) {
        if (policy == null || n.alarmId() == null || n.escalation() != null || !NotificationEvents.ALARM_RAISED.equals(n.event())) {
            return;
        }
        Optional<PolicyDefinition.Step> first = policy.step(1);
        if (first.isEmpty() || policy.step(2).isEmpty()) {
            return;
        }
        escalations.insert(organizationId, n.alarmId(), policy.policyId(), 2, now.plus(Duration.ofMinutes(first.get().waitMinutes())),
                Json.write(n), now, actionProperties.env());
    }

    // ───────────── 수신자 한 명 ─────────────

    private Optional<UUID> plan(long org, String requestKey, NotificationRequest n, String sourceType, String sourceId, AlarmInfo alarm,
                                PolicyDefinition policy, Target t, Instant now) {
        String recipientKey = n.escalation() == null ? t.recipientKey() : t.recipientKey() + "#step" + n.escalation().stepNo();
        String key = n.alarmId() != null
                ? ActionIdempotencyKeys.notificationDelivery(n.alarmId(), n.event(), recipientKey, t.channelKey(), n.eventSeq() == null ? 0 : n.eventSeq())
                : ActionIdempotencyKeys.notificationDelivery(requestKey, n.event(), recipientKey, t.channelKey());
        if (deliveries.findByKey(org, key).isPresent()) {
            return Optional.empty();   // 다시 처리된 요청: 발송은 한 번만(BR-RUL-17)
        }
        AlarmSeverity severity = n.severity() != null ? n.severity() : alarm == null ? null : alarm.severity();
        String locale = t.user() != null && t.user().locale() != null ? t.user().locale() : properties.defaultLocale();
        UUID id = UUID.randomUUID();
        MessagePayload payload = render(org, n, alarm, t, locale, severity, id);
        String skip = skipReason(org, n, alarm, policy, t, severity, now);
        DeliveryStatus status = DeliveryStatus.PENDING;
        String lastError = null;
        if (t.error() != null) {
            status = DeliveryStatus.FAILED;
            lastError = t.error() + ": " + t.channelKey() + " 채널이 등록되지 않았습니다";
        } else if (skip != null) {
            status = DeliveryStatus.SKIPPED;
        }
        Long aggregateId = null;
        Instant nextAt = status == DeliveryStatus.PENDING ? now : null;
        if (status == DeliveryStatus.PENDING) {
            Hold hold = hold(org, n, alarm, policy, t, severity, now);
            aggregateId = hold.aggregateId();
            nextAt = hold.sendNow() ? now : null;
        }
        Delivery d = new Delivery(id, org, key, t.channelId(), t.channelKey(), sourceType, sourceId, n.alarmId(), aggregateId, recipientKey,
                t.user() == null ? null : t.user().userId(), requestKey, n.event(), severity == null ? null : severity.name(),
                n.escalation() == null ? null : n.escalation().stepNo(), payload, 1, status, skip, 0, nextAt, lastError, null, now, null);
        if (!deliveries.insert(d, actionProperties.env())) {
            return Optional.empty();
        }
        if (status == DeliveryStatus.FAILED) {
            dispatcher.event(d, DeliveryStatus.FAILED, 0, lastError, now);
        }
        return status == DeliveryStatus.PENDING && nextAt != null ? Optional.of(id) : Optional.empty();
    }

    /** 건너뛸 사유(BR-RUL-12·13·14, SIM-07.04, OPS-06.05). 보내면 null */
    private String skipReason(long org, NotificationRequest n, AlarmInfo alarm, PolicyDefinition policy, Target t, AlarmSeverity severity,
                              Instant now) {
        if (t.skipReason() != null) {
            return t.skipReason();
        }
        if (alarm != null && alarm.flapping()) {
            return DeliverySkipReasons.FLAPPING;
        }
        if (alarm != null && (alarm.status() == AlarmStatus.SUPPRESSED || alarm.suppressedReason() != null)) {
            return DeliverySkipReasons.SUPPRESSED;
        }
        if (Boolean.TRUE.equals(n.virtual()) && properties.virtualMuted()) {
            return DeliverySkipReasons.VIRTUAL_MUTED;
        }
        if (alarm != null && SilenceMatcher.match(core.silences(org), alarm, now).isPresent()) {
            return DeliverySkipReasons.SILENCED;
        }
        if (t.user() != null && !t.user().wants(severity)) {
            return SKIP_SEVERITY;
        }
        if (policy != null && severity != null && severity != AlarmSeverity.UNKNOWN && !severity.atLeast(policy.minSeverity())) {
            return SKIP_SEVERITY;
        }
        if (n.alarmId() != null) {
            Duration renotify = policy == null ? properties.defaultRenotify() : Duration.ofMinutes(policy.renotifyMinutes());
            Instant last = deliveries.lastSentAt(org, n.alarmId(), t.recipientKey(), t.channelKey()).orElse(null);
            Optional<String> r = RenotifyPolicy.skip(n.event(), last, now, renotify);
            if (r.isPresent()) {
                return r.get();
            }
        }
        return null;
    }

    /** 최소 심각도 미달(OPS-06.05) */
    public static final String SKIP_SEVERITY = "SEVERITY";

    private record Hold(Long aggregateId, boolean sendNow) {
    }

    /** 방해 금지·묶음·한도 판단. 웹 알림은 바로 */
    private Hold hold(long org, NotificationRequest n, AlarmInfo alarm, PolicyDefinition policy, Target t, AlarmSeverity severity, Instant now) {
        if (t.channel() == null) {
            return new Hold(null, true);
        }
        ChannelDefinition ch = t.channel();
        Optional<Instant> dndEnd = t.user() == null ? Optional.empty() : t.user().dndUntil(now, severity);
        if (dndEnd.isPresent()) {
            Aggregate a = aggregates.lockOrOpen(org, ch.type(), ch.channelId(), t.recipientKey(), "dnd", KIND_DND, now, dndEnd.get(),
                    actionProperties.env());
            aggregates.count(org, a.id(), false);
            return new Hold(a.id(), false);
        }
        String aggKey = n.aggregateKey() != null ? n.aggregateKey()
                : alarm != null && alarm.ruleId() != null ? "rule:" + alarm.ruleId()
                : alarm != null && alarm.alarmKey() != null ? alarm.alarmKey() : n.event();
        int explicit = n.aggregateWindowSec() != null ? n.aggregateWindowSec() : policy == null ? 0 : policy.aggregateWindowSec();
        if (explicit > 0) {
            Aggregate a = aggregates.lockOrOpen(org, ch.type(), ch.channelId(), t.recipientKey(), aggKey, KIND_EXPLICIT, now,
                    now.plusSeconds(explicit), actionProperties.env());
            aggregates.count(org, a.id(), false);
            return new Hold(a.id(), false);
        }
        int window = ch.digestWindowSec() > 0 ? ch.digestWindowSec() : (int) properties.defaultDigestWindow().toSeconds();
        boolean overRate = ch.rateLimitPerMin() > 0
                && deliveries.countSentOrDueSince(org, ch.channelId(), now.minusSeconds(60)) >= ch.rateLimitPerMin();
        Aggregate a = aggregates.lockOrOpen(org, ch.type(), ch.channelId(), t.recipientKey(), overRate ? "rate" : aggKey, KIND_CHANNEL, now,
                now.plusSeconds(Math.max(window, 60)), actionProperties.env());
        boolean sendNow = DigestRule.sendNow(a.sentCount(), properties.digestThreshold(), overRate);
        aggregates.count(org, a.id(), sendNow);
        return new Hold(sendNow ? null : a.id(), sendNow);
    }

    /** 템플릿(core API-RUL-45 → 내장 기본), 버튼(ACK·30분 무음), 바로가기 링크(TC-RUL-078) */
    private MessagePayload render(long org, NotificationRequest n, AlarmInfo alarm, Target t, String locale, AlarmSeverity severity, UUID id) {
        Map<String, Object> vars = new LinkedHashMap<>();
        if (alarm != null) {
            vars.putAll(alarm.variables());
        }
        vars.putAll(n.variables());
        String link = n.link() != null ? n.link() : n.alarmId() != null ? properties.webBaseUrl() + "/alarms/" + n.alarmId() : null;
        if (link != null) {
            vars.put("link", link);
        }
        if (severity != null && !vars.containsKey("severity")) {
            vars.put("severity", severity.name());
        }
        String templateKey = n.templateKeyFor(t.channelKey());
        if (templateKey == null) {
            templateKey = NotificationEvents.defaultTemplateKey(n.event());
        }
        String[] tpl = core.template(org, templateKey, t.channelKey(), locale).orElseGet(() -> NotificationTexts.defaultTemplate(n.event(), locale));
        String title = TemplateRenderer.render(tpl[0] == null ? "{{title}}" : tpl[0], vars).trim();
        String body = TemplateRenderer.render(tpl[1], vars).replaceAll("\n\\s*\n", "\n").trim();
        if (Boolean.TRUE.equals(n.virtual())) {
            title = NotificationTexts.text("virtual", locale) + title;
        }
        List<ChannelButton> buttons = new ArrayList<>();
        boolean actionable = n.alarmId() != null && (NotificationEvents.ALARM_RAISED.equals(n.event())
                || NotificationEvents.ALARM_RERAISED.equals(n.event()) || NotificationEvents.ALARM_ESCALATED.equals(n.event()));
        if (actionable) {
            buttons.add(new ChannelButton(NotificationTexts.text("ack", locale), CallbackAction.ACK, n.alarmId(), id.toString()));
            buttons.add(new ChannelButton(NotificationTexts.text("mute", locale), CallbackAction.MUTE_30M, n.alarmId(), id.toString()));
        }
        return new MessagePayload(t.address(), title, body, link, buttons, locale, severity, null);
    }

    /** 처리 대상 수신자 정의(에스컬레이션 단계 등) */
    public static List<NotificationRecipient> stepRecipients(PolicyDefinition policy, PolicyDefinition.Step step) {
        return RecipientResolver.fromMembers(step.recipients(), policy.channels());
    }
}
