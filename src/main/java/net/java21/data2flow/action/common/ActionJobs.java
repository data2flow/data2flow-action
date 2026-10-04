package net.java21.data2flow.action.common;

import net.java21.data2flow.action.actuation.repository.DeviceStateRepository;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.actuation.service.CommandTracker;
import net.java21.data2flow.action.outbox.OutboxRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * 주기 작업. 시험은 {@code data2flow.action.scheduler.enabled=false}·{@code data2flow.action.outbox.relay-enabled=false}로 끄고 직접 부른다.
 * 여러 파드가 함께 돌아도 {@code FOR UPDATE SKIP LOCKED}로 한 행은 한 파드만 처리한다.
 */
public final class ActionJobs {

    private ActionJobs() {
    }

    /** 명령 기한·재시도·지연 실행(1초) */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class CommandDeadlines {

        private static final Logger log = LoggerFactory.getLogger(CommandDeadlines.class);
        private final CommandTracker tracker;

        CommandDeadlines(CommandTracker tracker) {
            this.tracker = tracker;
        }

        @Scheduled(fixedDelayString = "${data2flow.action.scheduler.period:1s}")
        void run() {
            try {
                tracker.processDue();
            } catch (RuntimeException e) {
                log.warn("명령 기한 작업 실패(다음 주기에 다시): {}", e.toString());
            }
        }
    }

    /** 아웃박스 릴레이(1초)와 보낸 행 정리(1시간) */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.outbox", name = "relay-enabled", havingValue = "true", matchIfMissing = true)
    static class Relay {

        private final OutboxRelay relay;

        Relay(OutboxRelay relay) {
            this.relay = relay;
        }

        @Scheduled(fixedDelayString = "${data2flow.action.outbox.interval:1s}")
        void run() {
            relay.relayOnce();
        }

        @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT5M")
        void purge() {
            relay.purgeSent();
        }
    }

    /** 제어 효과 확인(ACT-08.01, 30초)·일괄 제어 이어 하기(1분)·가동 집계(ACT-08.02, 1시간)·드라이버 호출 기록 정리(2일, 6시간) */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class ControlM4 {

        private static final Logger log = LoggerFactory.getLogger(ControlM4.class);
        private final net.java21.data2flow.action.actuation.service.ControlEffectService effects;
        private final net.java21.data2flow.action.actuation.service.BulkControlService bulk;
        private final net.java21.data2flow.action.actuation.service.RuntimeStatsService runtime;
        private final net.java21.data2flow.action.actuation.service.DriverHealthService health;

        ControlM4(net.java21.data2flow.action.actuation.service.ControlEffectService effects,
                  net.java21.data2flow.action.actuation.service.BulkControlService bulk,
                  net.java21.data2flow.action.actuation.service.RuntimeStatsService runtime,
                  net.java21.data2flow.action.actuation.service.DriverHealthService health) {
            this.effects = effects;
            this.bulk = bulk;
            this.runtime = runtime;
            this.health = health;
        }

        @Scheduled(fixedDelayString = "PT30S", initialDelayString = "PT30S")
        void effects() {
            try {
                effects.processDue();
            } catch (RuntimeException e) {
                log.warn("효과 확인 작업 실패: {}", e.toString());
            }
        }

        @Scheduled(fixedDelayString = "PT1M", initialDelayString = "PT1M")
        void resumeBulk() {
            try {
                bulk.resumeStale();
            } catch (RuntimeException e) {
                log.warn("일괄 제어 이어 하기 실패: {}", e.toString());
            }
        }

        @Scheduled(fixedDelayString = "PT1H", initialDelayString = "PT2M")
        void runtime() {
            try {
                runtime.aggregateRecent();
            } catch (RuntimeException e) {
                log.warn("가동 집계 실패: {}", e.toString());
            }
        }

        @Scheduled(fixedDelayString = "PT6H", initialDelayString = "PT3M")
        void purgeDriverCalls() {
            try {
                health.purge();
            } catch (RuntimeException e) {
                log.warn("드라이버 호출 기록 정리 실패: {}", e.toString());
            }
        }
    }

    /** 보관 정리(처리 기록 7일)와 상태 구간 월 파티션 미리 만들기 */
    @Component
    @ConditionalOnProperty(prefix = "data2flow.action.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class Housekeeping {

        private static final Logger log = LoggerFactory.getLogger(Housekeeping.class);
        private final ExecutionRepository executions;
        private final DeviceStateRepository deviceState;
        private final Clock clock;

        Housekeeping(ExecutionRepository executions, DeviceStateRepository deviceState, Clock clock) {
            this.executions = executions;
            this.deviceState = deviceState;
            this.clock = clock;
        }

        @Scheduled(fixedDelayString = "PT6H", initialDelayString = "PT1M")
        void run() {
            try {
                executions.deleteProcessedBefore(clock.instant().minus(Duration.ofDays(7)));
                LocalDate month = LocalDate.ofInstant(clock.instant(), ZoneOffset.UTC).withDayOfMonth(1);
                deviceState.ensureMonthPartition(month);
                deviceState.ensureMonthPartition(month.plusMonths(1));
            } catch (RuntimeException e) {
                log.warn("보관 정리·파티션 작업 실패: {}", e.toString());
            }
        }
    }
}
