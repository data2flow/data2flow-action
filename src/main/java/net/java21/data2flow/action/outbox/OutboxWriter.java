package net.java21.data2flow.action.outbox;

import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.audit.AuditEvent;
import net.java21.data2flow.contracts.audit.AuditRecorder;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.event.EventPayload;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.UUID;

/**
 * 아웃박스 기록. 이벤트(EVT-ACT-01·02)는 {@code data2flow.events}로, 감사(IAM-06.01)는 core 내부 콜백 {@code POST /internal/core/audit-logs}
 * (API-IAM-39)로 보낸다. 상태 변경과 같은 트랜잭션에서 부르면 둘 다 잃지 않는다(ADR-020, reliability-and-ha.md ⑦).
 *
 * <p>감사 기록 창구({@link AuditRecorder})도 이것이다. 제어 명령 감사는 명령과 같은 트랜잭션에 남으므로 core가 잠시 멈춰도 사라지지 않는다.
 */
@Component
public class OutboxWriter implements AuditRecorder {

    public static final String KIND_EVENT = "EVENT";
    public static final String KIND_CALLBACK = "CALLBACK";
    /** CALLBACK의 exchange 칸: core 내부 API로 보낸다 */
    public static final String CORE_CALLBACK = "core";
    public static final String AUDIT_PATH = "/internal/core/audit-logs";

    private final OutboxRepository repository;
    private final MessageCodec codec = MessageCodec.create();
    private final Clock clock;
    private final String env;

    public OutboxWriter(OutboxRepository repository, Clock clock, ActionProperties properties) {
        this.repository = repository;
        this.clock = clock;
        this.env = properties.env();
    }

    /** 도메인 이벤트. {@code dedupKey}가 같으면 한 번만 기록된다 */
    public <P extends EventPayload> DomainEvent<P> event(EventType type, long organizationId, P payload, String dedupKey) {
        DomainEvent<P> event = DomainEvent.of(type, organizationId, payload, MDC.get("requestId"), clock);
        repository.insert(organizationId, ActionIdempotencyKeys.of("event", dedupKey), KIND_EVENT, MessagingNames.EXCHANGE_EVENTS,
                event.type(), codec.writeAsString(event), clock.instant(), env);
        return event;
    }

    /** 감사 기록(API-IAM-39 본문) */
    @Override
    public void record(AuditEvent event) {
        repository.insert(event.organizationId(), ActionIdempotencyKeys.of("audit", UUID.randomUUID().toString()), KIND_CALLBACK,
                CORE_CALLBACK, AUDIT_PATH, Json.write(event), clock.instant(), env);
    }
}
