package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.RetryPolicy;
import net.java21.data2flow.action.actuation.driver.DeviceDriver;
import net.java21.data2flow.action.actuation.driver.DriverCommand;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverDevice;
import net.java21.data2flow.action.actuation.driver.DriverRegistry;
import net.java21.data2flow.action.actuation.driver.DriverResult;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.CommandStatusReasons;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * 드라이버 호출(제어 창구의 마지막 단계, BR-ACT-01). 창구({@link ControlFacade})와 기한 작업만 부른다.
 *
 * <ol>
 *   <li>잠금 트랜잭션: REQUESTED이고 기한이 된 명령만, 호출 횟수를 올리고 기한을 ack 기한으로 미뤄 다른 파드가 겹쳐 부르지 않게 한다.</li>
 *   <li>트랜잭션 밖에서 드라이버를 부른다(HTTP·MQTT). 같은 commandId는 드라이버·장비가 한 번만 적용한다.</li>
 *   <li>결과 트랜잭션: ACCEPTED → SENT, ACKED → ACKED, 일시 실패 → 지수 백오프 뒤 다시(BR-ACT-14, 최대 3회), 그 밖 → FAILED(reason).
 *       ack가 호출 응답보다 먼저 와서 이미 ACKED·APPLIED면 되돌리지 않는다.</li>
 * </ol>
 * 호출 중 파드가 죽으면 기한 작업이 ack 기한 뒤 다시 부른다(최소 1회 + 드라이버 멱등).
 * 서킷 브레이커(ACT-07.03, BR-ACT-14): 드라이버 서킷이 열려 있으면 부르지 않고 FAILED(DRIVER_UNAVAILABLE), 호출마다 지표를 남긴다(ACT-03.06).
 */
public class CommandDispatcher {

    private static final Logger log = LoggerFactory.getLogger(CommandDispatcher.class);

    private final CommandRepository commands;
    private final ShadowRepository shadows;
    private final CommandEvents events;
    private final ControlProfileService profiles;
    private final DriverRegistry drivers;
    private final CommandWaiter waiter;
    private final TransactionTemplate tx;
    private final ActionProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;
    private final DriverHealthService health;

    public CommandDispatcher(CommandRepository commands, ShadowRepository shadows, CommandEvents events, ControlProfileService profiles,
                             DriverRegistry drivers, CommandWaiter waiter, PlatformTransactionManager txManager,
                             ActionProperties properties, MeterRegistry meters, Clock clock, DriverHealthService health) {
        this.commands = commands;
        this.shadows = shadows;
        this.events = events;
        this.profiles = profiles;
        this.drivers = drivers;
        this.waiter = waiter;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
        this.health = health;
    }

    private record Call(Command command, DeviceDriver driver, DriverCommand driverCommand, DriverBinding binding, Long spaceId) {
    }

    /** 명령 하나를 드라이버로 보낸다. 보낼 상태가 아니면 아무것도 하지 않는다 */
    public void dispatch(UUID commandId) {
        Call call = tx.execute(status -> prepare(commandId));
        if (call == null) {
            return;
        }
        long organizationId = call.command().organizationId();
        net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker.Admission admission = health.admit(organizationId, call.binding());
        if (admission == net.java21.data2flow.action.actuation.domain.DriverCircuitBreaker.Admission.REJECT) {
            meters.counter("data2flow_action_driver_circuit_rejected_total", "type", call.driver().type()).increment();
            tx.executeWithoutResult(status -> commands.lock(commandId).filter(c -> c.status() == CommandStatus.REQUESTED).ifPresent(c ->
                    events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.DRIVER_UNAVAILABLE, clock.instant(), null),
                            call.spaceId(), "드라이버 서킷이 열려 있습니다")));
            waiter.signal(commandId);
            return;
        }
        Timer.Sample sample = Timer.start(meters);
        long started = System.nanoTime();
        DriverResult result;
        try {
            result = call.driver().execute(call.driverCommand());
        } catch (RuntimeException e) {
            // 계약 위반(예외)도 일시 실패로 다룬다
            log.warn("드라이버 예외 type={} command={}: {}", call.driver().type(), commandId, e.toString());
            result = DriverResult.failed(CommandStatusReasons.DRIVER_ERROR, true, e.getMessage());
        }
        long latencyMs = (System.nanoTime() - started) / 1_000_000;
        sample.stop(meters.timer("data2flow_action_driver_calls", "type", call.driver().type(), "result", result.status().name()));
        // 드라이버 장애(일시 실패)만 서킷 실패로 센다. 장비가 명령을 거부한 것(4xx)은 드라이버 장애가 아니다
        boolean driverFailure = result.status() == DriverResult.Status.FAILED && result.retryable();
        Object message = result.detail().get("message");
        health.record(organizationId, call.binding(), commandId, !driverFailure, latencyMs,
                driverFailure ? (message == null ? result.reason() : message.toString()) : null);
        DriverResult r = result;
        tx.executeWithoutResult(status -> complete(call, r));
        waiter.signal(commandId);
    }

    private Call prepare(UUID commandId) {
        Optional<Command> locked = commands.lock(commandId);
        if (locked.isEmpty() || locked.get().status() != CommandStatus.REQUESTED) {
            return null;
        }
        Command c = locked.get();
        Instant now = clock.instant();
        if (c.timeoutAt() != null && c.timeoutAt().isAfter(now)) {
            return null;   // 다른 파드가 이미 부르는 중이거나 재시도 시각 전
        }
        Optional<ControlProfile> profile = profiles.find(c.deviceId());
        Long spaceId = profile.map(ControlProfile::spaceId).orElse(null);
        if (c.validUntil() != null && now.isAfter(c.validUntil())) {
            events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.EXPIRED, now, null), spaceId, null);
            return null;
        }
        DriverBinding binding = profile.map(ControlProfile::driver).orElse(null);
        Optional<DeviceDriver> driver = binding == null ? Optional.empty() : drivers.find(binding.type());
        if (driver.isEmpty()) {
            log.warn("드라이버가 없습니다 command={} type={}", c.id(), binding == null ? null : binding.type());
            events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, CommandStatusReasons.DRIVER_UNAVAILABLE, now, null), spaceId, null);
            return null;
        }
        ControlProfile p = profile.get();
        long desiredVersion = shadows.findByDeviceIdAndOrganizationId(c.deviceId(), c.organizationId())
                .map(row -> row.shadow().desiredVersion()).orElse(0L);
        Duration ack = ackTimeout(binding, driver.get());
        commands.save(CommandEvents.attempted(c, now.plus(ack)));
        DriverConfig config = new DriverConfig(binding.driverId(), binding.type(), binding.config(), binding.secrets());
        DriverDevice device = new DriverDevice(c.deviceId(), c.organizationId(), p.externalId(), p.virtual(), config);
        DriverCommand dc = new DriverCommand(c.id(), device, c.capability(), c.command(), c.args(), c.validUntil(), c.idempotencyKey(),
                desiredVersion);
        return new Call(CommandEvents.attempted(c, now.plus(ack)), driver.get(), dc, binding, spaceId);
    }

    private void complete(Call call, DriverResult result) {
        Optional<Command> locked = commands.lock(call.command().id());
        if (locked.isEmpty()) {
            return;
        }
        Command c = locked.get();
        Instant now = clock.instant();
        if (!result.detail().isEmpty()) {
            commands.saveDriverResponse(c.id(), Json.write(result.detail()));
        }
        if (c.status() != CommandStatus.REQUESTED) {
            return;   // ack·상태 보고가 먼저 왔다(이미 ACKED·APPLIED) 또는 다른 경로가 끝냈다
        }
        Duration ack = ackTimeout(call.binding(), call.driver());
        switch (result.status()) {
            case ACCEPTED -> events.transition(c, CommandEvents.to(c, CommandStatus.SENT, null, now, now.plus(ack)), call.spaceId(), null);
            case ACKED -> {
                Command sent = events.transition(c, CommandEvents.to(c, CommandStatus.SENT, null, now, now.plus(ack)), call.spaceId(), null);
                Duration apply = call.binding().applyTimeout(properties.command().applyTimeout());
                events.transition(sent, CommandEvents.to(sent, CommandStatus.ACKED, null, now, now.plus(apply)), call.spaceId(), null);
            }
            case FAILED -> {
                RetryPolicy retry = call.binding().retry() == null
                        ? new RetryPolicy(properties.command().retryMaxAttempts(), properties.command().retryInitial().toMillis(),
                        properties.command().retryMultiplier(), properties.command().retryMax().toMillis())
                        : call.binding().retry();
                if (result.retryable() && retry.canRetry(c.attempts())) {
                    commands.save(new Command(c.id(), c.organizationId(), c.idempotencyKey(), c.deviceId(), c.capability(), c.command(),
                            c.args(), c.priority(), c.source(), c.status(), result.reason(), c.validUntil(), c.executeAfter(), c.attempts(),
                            c.requestedAt(), c.sentAt(), c.ackedAt(), c.appliedAt(), c.finishedAt(), now.plus(retry.backoff(c.attempts()))));
                } else {
                    String reason = result.reason() == null ? CommandStatusReasons.DRIVER_ERROR : result.reason();
                    events.transition(c, CommandEvents.to(c, CommandStatus.FAILED, reason, now, null), call.spaceId(),
                            result.detail().get("message") == null ? null : result.detail().get("message").toString());
                }
            }
        }
    }

    private Duration ackTimeout(DriverBinding binding, DeviceDriver driver) {
        Duration configured = binding.ackTimeout(properties.command().ackTimeout());
        return configured.compareTo(driver.minResponseTimeout()) < 0 ? driver.minResponseTimeout() : configured;
    }
}
