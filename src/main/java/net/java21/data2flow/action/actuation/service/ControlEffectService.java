package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlEffects;
import net.java21.data2flow.action.actuation.domain.MetricValue;
import net.java21.data2flow.action.actuation.repository.EffectCheckRepository;
import net.java21.data2flow.action.actuation.repository.RuntimeStatRepository;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.action.outbox.OutboxWriter;
import net.java21.data2flow.contracts.capability.CapabilityCatalog;
import net.java21.data2flow.contracts.capability.ExpectedEffect;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.CommandNoEffect;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;

/**
 * 제어 효과 확인(ACT-08.01, BR-ACT-20). 명령이 APPLIED되면 기능의 기대 효과(예: 냉방 → 15분 안에 temperature 하강)를 찾아 확인을 예약하고,
 * 기한이 되면 기기 공간의 측정값(core API-ACT-45, APPLIED 시각과 기한 시각의 값)을 비교한다. 기대 방향으로 {@code minChange} 이상 바뀌지 않으면
 * EVT-ACT-04 {@code command.no-effect}를 낸다(같은 기기 1시간에 1회). 측정값이 없으면 판정하지 않는다(UNKNOWN).
 */
public class ControlEffectService {

    private static final Logger log = LoggerFactory.getLogger(ControlEffectService.class);
    /** 같은 기기 효과 없음 이벤트 간격(BR-ACT-20) */
    public static final Duration EVENT_INTERVAL = Duration.ofHours(1);

    private final EffectCheckRepository checks;
    private final RuntimeStatRepository runtime;
    private final CoreClient core;
    private final OutboxWriter outbox;
    private final TransactionTemplate tx;
    private final ActionProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;
    private final CapabilityCatalog catalog = CapabilityCatalog.standard();

    public ControlEffectService(EffectCheckRepository checks, RuntimeStatRepository runtime, CoreClient core, OutboxWriter outbox,
                                PlatformTransactionManager txManager, ActionProperties properties, MeterRegistry meters, Clock clock) {
        this.checks = checks;
        this.runtime = runtime;
        this.core = core;
        this.outbox = outbox;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
    }

    /** APPLIED 전이와 같은 트랜잭션에서 부른다. 기대 효과가 없으면 아무것도 하지 않는다 */
    public void schedule(Command c, Long spaceId, Instant appliedAt) {
        Optional<ExpectedEffect> effect = ControlEffects.expected(catalog, c.capability(), c.command(), c.args());
        if (effect.isEmpty() || effect.get().withinMinutes() <= 0) {
            return;
        }
        ExpectedEffect e = effect.get();
        checks.insert(new EffectCheckRepository.Check(c.id(), c.organizationId(), c.deviceId(), spaceId, c.capability(), e.metric(),
                e.direction().name(), e.withinMinutes(), appliedAt, appliedAt.plus(Duration.ofMinutes(e.withinMinutes())), "PENDING",
                null, null, null), properties.env());
    }

    /** 기한이 된 확인을 처리한다(주기 작업). 처리한 건수 */
    public int processDue() {
        int n = 0;
        for (EffectCheckRepository.Check due : checks.findDue(clock.instant(), properties.scheduler().batch(), properties.env())) {
            try {
                Double start = value(due, due.startAt());
                Double end = value(due, due.dueAt());
                finish(due, start, end);
                n++;
            } catch (RuntimeException e) {
                log.warn("효과 확인 실패(다음 주기에 다시) command={}: {}", due.commandId(), e.toString());
            }
        }
        return n;
    }

    private Double value(EffectCheckRepository.Check c, Instant at) {
        if (c.spaceId() == null) {
            return core.metricValue(c.metric(), c.deviceId(), null, at).map(MetricValue::value).orElse(null);
        }
        return core.metricValue(c.metric(), null, c.spaceId(), at).map(MetricValue::value).orElse(null);
    }

    private void finish(EffectCheckRepository.Check due, Double start, Double end) {
        ExpectedEffect.Direction direction = ExpectedEffect.Direction.valueOf(due.direction());
        ControlEffects.Verdict verdict = ControlEffects.judge(direction, start, end, properties.effect().minChange());
        tx.executeWithoutResult(s -> checks.lockPending(due.commandId()).ifPresent(c -> {
            Instant now = clock.instant();
            boolean emit = false;
            if (verdict == ControlEffects.Verdict.NO_EFFECT) {
                emit = !checks.emittedSince(c.deviceId(), now.minus(EVENT_INTERVAL));
                if (emit) {
                    outbox.event(EventType.COMMAND_NO_EFFECT, c.organizationId(), new CommandNoEffect(c.commandId(), c.deviceId(), c.spaceId(),
                                    c.capability(), new CommandNoEffect.Expected(c.metric(), direction, c.withinMinutes()),
                                    CommandNoEffect.Observed.between(start, end), now),
                            "no-effect:" + c.commandId());
                }
                runtime.incrementNoEffect(c.organizationId(), c.deviceId(), LocalDate.ofInstant(now, zone()));
                meters.counter("data2flow_action_no_effect_total").increment();
            }
            checks.finish(c.commandId(), verdict.name(), start, end, emit, now);
        }));
    }

    private ZoneId zone() {
        return properties.effect().zone();
    }
}
