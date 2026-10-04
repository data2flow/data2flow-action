package net.java21.data2flow.action.actuation.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.ControlSettings;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.actuation.domain.Protection;
import net.java21.data2flow.action.actuation.domain.ProtectionState;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.DeviceStateRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository.ShadowRow;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.audit.AuditRecorder;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.capability.AttributeConstraint;
import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.SourceType;
import net.java21.data2flow.contracts.error.BusinessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

/**
 * 제어 창구 검사 순서(BR-ACT-01)와 단계별 결과. 저장소·드라이버는 대역이다. TC-ACT-021·022·023·025·116·117
 */
class ControlFacadeTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final CommandRepository commands = mock(CommandRepository.class);
    private final ShadowRepository shadows = mock(ShadowRepository.class);
    private final DeviceStateRepository deviceState = mock(DeviceStateRepository.class);
    private final CommandEvents events = mock(CommandEvents.class);
    private final ControlProfileService profiles = mock(ControlProfileService.class);
    private final SandboxRegistry sandbox = mock(SandboxRegistry.class);
    private final RoleChecker roleChecker = mock(RoleChecker.class);
    private final AuditRecorder audit = mock(AuditRecorder.class);
    private final CommandDispatcher dispatcher = mock(CommandDispatcher.class);
    private final EmergencyStopRegistry emergency = mock(EmergencyStopRegistry.class);
    private final InterlockService interlocks = mock(InterlockService.class);
    private ControlFacade facade;
    private ShadowRow shadow;

    @BeforeEach
    void setUp() {
        facade = new ControlFacade(commands, shadows, deviceState, events, profiles, sandbox, roleChecker, audit, dispatcher,
                mock(PlatformTransactionManager.class), new ActionProperties(null, null, null, null, null, null, null, null, null, null),
                new SimpleMeterRegistry(), clock, emergency, interlocks);
        given(interlocks.evaluate(any(), any(), anyString(), anyString(), any()))
                .willReturn(net.java21.data2flow.action.actuation.domain.InterlockEvaluator.Decision.PASS);
        given(commands.findByKey(anyLong(), anyString())).willReturn(Optional.empty());
        given(commands.insert(any(), anyString())).willReturn(true);
        given(commands.findSentSince(anyLong(), anyString(), any(), any())).willReturn(List.of());
        given(commands.lastSentAt(anyLong(), anyString(), any())).willReturn(Optional.empty());
        given(commands.lockPending(anyLong(), anyString(), any())).willReturn(List.of());
        given(commands.findByIdAndOrganizationId(any(), anyLong())).willReturn(Optional.empty());
        given(events.transition(any(), any(), any(), any())).willAnswer(inv -> inv.getArgument(1));
        given(profiles.find(Fixtures.AIRCON)).willReturn(Optional.of(profile(false)));
        given(sandbox.spaces()).willReturn(Set.of(Fixtures.SPACE));
        given(deviceState.findManualOverride(anyLong(), anyLong(), anyString())).willReturn(Optional.empty());
        given(deviceState.findProtection(anyLong(), anyLong())).willReturn(ProtectionState.EMPTY);
        online(DeviceShadow.EMPTY);
    }

    /** 실제 에어컨: 모델 16~30, 조직 한계 18~28, 압축기 보호 */
    private static ControlProfile profile(boolean virtual) {
        ModelCapability thermostat = new ModelCapability(Map.of("targetTemperature", AttributeConstraint.range(16, 30)),
                Protection.COMPRESSOR, false, false);
        Map<String, ModelCapability> caps = new LinkedHashMap<>();
        caps.put("Thermostat", thermostat);
        caps.put("Switch", ModelCapability.PLAIN);
        return new ControlProfile(Fixtures.AIRCON, Fixtures.ORG, Fixtures.SPACE, "에어컨", "ac-15", virtual, "ACTIVE", 3L, caps,
                new DriverBinding(9L, "VIRTUAL", Map.of(), 30, 60, null), Fixtures.settings());
    }

    private void online(DeviceShadow s) {
        shadow = new ShadowRow(Fixtures.AIRCON, Fixtures.ORG, s, null, null, "ONLINE");
        given(shadows.lockOrCreate(anyLong(), anyLong())).willAnswer(inv -> shadow);
    }

    private CommandRequest request(SourceType type, Map<String, Object> args, Long sourceSpace) {
        CommandSource source = switch (type) {
            case USER -> CommandSource.user(Fixtures.USER);
            case FLOW -> CommandSource.flow("f-1", 13, "n-act-1", "m-" + UUID.randomUUID());
            case SCHEDULE -> CommandSource.schedule(4);
            case AI -> CommandSource.ai("s-1", Fixtures.USER);
            case SYSTEM -> CommandSource.system();
            default -> throw new IllegalArgumentException();
        };
        return new CommandRequest(Fixtures.ORG, Fixtures.AIRCON, "Thermostat", "set", args, source, null, "k-" + UUID.randomUUID(), null,
                null, sourceSpace, false, false);
    }

    static Stream<Arguments> pipeline() {
        return Stream.of(
                Arguments.of("샌드박스 공간 플로우 → 실제 기기", SourceType.FLOW, Map.of("mode", "cool"), 31L, "REJECTED", "SANDBOX_FORBIDDEN"),
                Arguments.of("기능 스키마 위반(모르는 인자)", SourceType.USER, Map.of("speed", 3), null, "REJECTED", "ARGS_INVALID"),
                Arguments.of("모델 제약 밖(31℃)", SourceType.USER, Map.of("targetTemperature", 31), null, "REJECTED", "MODEL_CONSTRAINT"),
                Arguments.of("조직 절대 한계 밖(29℃)", SourceType.USER, Map.of("targetTemperature", 29), null, "REJECTED", "ABSOLUTE_LIMIT"),
                Arguments.of("통과(24℃)", SourceType.USER, Map.of("mode", "cool", "targetTemperature", 24), null, "REQUESTED", null));
    }

    @ParameterizedTest(name = "{0} → {4}({5})")
    @MethodSource("pipeline")
    @DisplayName("[ACT-02.01][BR-ACT-01][TC-ACT-022] 검사 순서표: 앞 단계에서 걸리면 기록하고 드라이버를 부르지 않는다")
    void pipelineTable(String name, SourceType type, Map<String, Object> args, Long sourceSpace, String status, String reason) {
        Outcome o = facade.submit(request(type, args, sourceSpace));

        assertThat(o.command().status().name()).isEqualTo(status);
        assertThat(o.command().statusReason()).isEqualTo(reason);
        then(audit).should().record(any());
        if ("REQUESTED".equals(status)) {
            then(dispatcher).should().dispatch(o.command().id());
            then(shadows).should().save(any(), any());
        } else {
            then(dispatcher).shouldHaveNoInteractions();
            assertThat(o.rejection()).isNotNull();
        }
    }

    @Test
    @DisplayName("[ACT-02.01][BR-ACT-01][TC-ACT-025] 위반이 겹치면 앞 단계 사유: 샌드박스+한계 → SANDBOX, 한계+수동 우선 → ABSOLUTE_LIMIT, 수동 우선+변경 없음 → MANUAL_OVERRIDE")
    void earlierStageWins() {
        Outcome sandboxAndLimit = facade.submit(request(SourceType.FLOW, Map.of("targetTemperature", 29), Fixtures.SPACE));
        assertThat(sandboxAndLimit.command().statusReason()).isEqualTo("SANDBOX_FORBIDDEN");
        assertThat(sandboxAndLimit.rejection().code()).isEqualTo(ActionErrorCode.ACT_SANDBOX_FORBIDDEN);

        given(deviceState.findManualOverride(anyLong(), anyLong(), eq("Thermostat"))).willReturn(Optional.of(
                new DeviceStateRepository.ManualOverride("Thermostat", clock.instant().plus(Duration.ofMinutes(20)), Fixtures.USER)));
        Outcome limitAndOverride = facade.submit(request(SourceType.FLOW, Map.of("targetTemperature", 29), null));
        assertThat(limitAndOverride.command().statusReason()).isEqualTo("ABSOLUTE_LIMIT");

        online(DeviceShadow.EMPTY.withDesired("Thermostat", Map.of("mode", "cool"))
                .withReported(1, Map.of("Thermostat", Map.of("mode", "cool")), clock.instant()).orElseThrow());
        Outcome overrideAndNoChange = facade.submit(request(SourceType.FLOW, Map.of("mode", "cool"), null));
        assertThat(overrideAndNoChange.command().status()).isEqualTo(CommandStatus.SKIPPED);
        assertThat(overrideAndNoChange.command().statusReason()).isEqualTo("MANUAL_OVERRIDE");

        then(dispatcher).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("[ACT-05.03][BR-ACT-04] desired·reported가 이미 목표와 같으면 SKIPPED(NO_CHANGE), 드라이버 미호출")
    void noChange() {
        online(DeviceShadow.EMPTY.withDesired("Thermostat", Map.of("mode", "cool"))
                .withReported(1, Map.of("Thermostat", Map.of("mode", "cool")), clock.instant()).orElseThrow());

        Outcome o = facade.submit(request(SourceType.USER, Map.of("mode", "cool"), null));

        assertThat(o.command().statusReason()).isEqualTo("NO_CHANGE");
        then(dispatcher).shouldHaveNoInteractions();
    }

    static Stream<Arguments> limits() {
        return Stream.of(Arguments.of(18.0, true), Arguments.of(28.0, true), Arguments.of(17.5, false), Arguments.of(28.5, false));
    }

    @ParameterizedTest(name = "{0}℃ → 허용 {1}")
    @MethodSource("limits")
    @DisplayName("[ACT-06.04][AT-ACT-01.3][TC-ACT-117] 조직 절대 한계 18~28 경계값, 출처 MANUAL·SAFETY·AI·SCHEDULE 모두 같은 결과")
    void absoluteLimitBoundaries(double temperature, boolean allowed) {
        for (SourceType type : List.of(SourceType.USER, SourceType.SYSTEM, SourceType.AI, SourceType.SCHEDULE)) {
            Outcome o = facade.submit(request(type, Map.of("targetTemperature", temperature), null));
            if (allowed) {
                assertThat(o.rejection()).as(type.name()).isNull();
            } else {
                assertThat(o.command().statusReason()).as(type.name()).isEqualTo("ABSOLUTE_LIMIT");
                assertThat(o.rejection().code()).isEqualTo(ActionErrorCode.COMMAND_ABSOLUTE_LIMIT);
                assertThat(o.rejection().args()).containsExactly("18", "28");
            }
        }
    }

    @Test
    @DisplayName("[ACT-06.04][AT-ACT-08.3][TC-ACT-115·116] 조직 한계는 모델 제약보다 좁게만: 16~28은 모델 18~30보다 넓다")
    void limitWithinModel() {
        AttributeConstraint model = AttributeConstraint.range(18, 30);
        assertThat(AttributeConstraint.range(16, 28).within(model)).isFalse();
        assertThat(AttributeConstraint.range(18, 28).within(model)).isTrue();
        assertThat(model.intersect(AttributeConstraint.range(18, 28))).isEqualTo(AttributeConstraint.range(18, 28));
    }

    @Test
    @DisplayName("[SIM-07.03][AT-ACT-15.3][TC-ACT-021] 샌드박스 해제 뒤 같은 명령은 실제 기기 제어 허용, 가상 기기는 언제나 허용")
    void sandboxReleased() {
        given(sandbox.spaces()).willReturn(Set.of());
        assertThat(facade.submit(request(SourceType.FLOW, Map.of("mode", "cool"), Fixtures.SPACE)).rejection()).isNull();

        given(sandbox.spaces()).willReturn(Set.of(Fixtures.SPACE));
        given(profiles.find(Fixtures.AIRCON)).willReturn(Optional.of(profile(true)));
        clock.advanceBy(Duration.ofSeconds(11));
        assertThat(facade.submit(request(SourceType.FLOW, Map.of("mode", "heat"), Fixtures.SPACE)).rejection()).isNull();
    }

    @Test
    @DisplayName("[ACT-02.01][BR-ACT-02][TC-ACT-023] 같은 멱등 키는 한 번만 실행하고 처음 결과를 돌려준다(거부된 명령은 같은 오류)")
    void idempotentReplay() {
        Command first = Fixtures.command(UUID.randomUUID(), "Thermostat", Map.of("targetTemperature", 29), CommandPriority.MANUAL,
                CommandStatus.REJECTED, clock.instant());
        Command rejected = new Command(first.id(), first.organizationId(), first.idempotencyKey(), first.deviceId(), first.capability(),
                first.command(), first.args(), first.priority(), first.source(), CommandStatus.REJECTED, "ABSOLUTE_LIMIT",
                first.validUntil(), null, 0, first.requestedAt(), null, null, null, first.requestedAt(), null);
        given(commands.findByKey(Fixtures.ORG, "same")).willReturn(Optional.of(rejected));

        Outcome o = facade.submit(new CommandRequest(Fixtures.ORG, Fixtures.AIRCON, "Thermostat", "set", Map.of("targetTemperature", 29),
                CommandSource.user(Fixtures.USER), null, "same", null, null, null, true, false));

        assertThat(o.replay()).isTrue();
        assertThat(o.command().id()).isEqualTo(first.id());
        assertThat(o.rejection().code()).isEqualTo(ActionErrorCode.COMMAND_ABSOLUTE_LIMIT);
        then(commands).should(never()).insert(any(), anyString());
        then(dispatcher).shouldHaveNoInteractions();
        assertThat(ControlFacade.replayRejection(Fixtures.command(UUID.randomUUID(), "Switch", Map.of(), CommandPriority.AUTO,
                CommandStatus.APPLIED, clock.instant()))).isNull();
    }

    @Test
    @DisplayName("[ACT-02.05][TC-ACT-052] 60초 안 반대 명령이 세 번째면 BLOCKED(OSCILLATION) → 409 COMMAND_BLOCKED")
    void oscillationBlocked() {
        Instant t = clock.instant();
        given(commands.findSentSince(anyLong(), eq("Thermostat"), any(), any())).willReturn(List.of(
                Fixtures.command(UUID.randomUUID(), "Thermostat", Map.of("mode", "cool"), CommandPriority.MANUAL, CommandStatus.APPLIED, t),
                Fixtures.command(UUID.randomUUID(), "Thermostat", Map.of("mode", "off"), CommandPriority.MANUAL, CommandStatus.APPLIED, t)));

        Outcome o = facade.submit(request(SourceType.USER, Map.of("mode", "cool"), null));

        assertThat(o.command().status()).isEqualTo(CommandStatus.BLOCKED);
        assertThat(o.command().statusReason()).isEqualTo("OSCILLATION");
        assertThat(o.rejection().code()).isEqualTo(ActionErrorCode.COMMAND_BLOCKED);
        // EVT-ACT-08: 진동 차단 WARNING 알람의 원천(ADR-048)
        then(events).should().oscillationBlocked(any(), eq(Fixtures.SPACE), eq(3), eq(60));
    }

    // ───────────── M4: 비상 정지·인터락·대기열·Class A ─────────────

    @Test
    @DisplayName("[ACT-06.03][AT-ACT-09.1][TC-ACT-109] 비상 정지 범위 안의 AUTO 명령 → SKIPPED(EMERGENCY_STOP), 드라이버 호출 없음")
    void emergencyStopSkipsAutomatic() {
        var stop = new net.java21.data2flow.action.actuation.domain.EmergencyStop(1, Fixtures.ORG,
                net.java21.data2flow.contracts.message.event.EmergencyStopChanged.Scope.space(Fixtures.SPACE), "점검", clock.instant());
        given(emergency.blocking(eq(Fixtures.ORG), eq(CommandPriority.AUTO), any())).willReturn(Optional.of(stop));

        Outcome o = facade.submit(request(SourceType.FLOW, Map.of("mode", "cool"), null));

        assertThat(o.command().status()).isEqualTo(CommandStatus.SKIPPED);
        assertThat(o.command().statusReason()).isEqualTo("EMERGENCY_STOP");
        then(dispatcher).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("[ACT-06.02][AT-ACT-08.2][TC-ACT-101] 인터락 조건 참 → BLOCKED(INTERLOCK) + 인터락 message, 타임라인에 인터락 ID")
    void interlockBlocks() {
        var il = new net.java21.data2flow.action.actuation.domain.Interlock(5L, "창문", 31L, true, null, null, "창문이 열려 있습니다", null);
        given(interlocks.evaluate(any(), any(), anyString(), anyString(), any())).willReturn(
                new net.java21.data2flow.action.actuation.domain.InterlockEvaluator.Decision(il,
                        net.java21.data2flow.action.actuation.domain.InterlockEvaluator.Reason.CONDITION_TRUE));
        given(events.transition(any(), any(), any(), any(), any())).willAnswer(inv -> inv.getArgument(1));

        Outcome o = facade.submit(request(SourceType.USER, Map.of("mode", "cool"), null));

        assertThat(o.command().status()).isEqualTo(CommandStatus.BLOCKED);
        assertThat(o.command().statusReason()).isEqualTo("INTERLOCK");
        assertThat(o.message()).isEqualTo("창문이 열려 있습니다");
        assertThat(o.rejection().code()).isEqualTo(ActionErrorCode.COMMAND_BLOCKED);
        then(events).should().transition(any(), any(), eq(Fixtures.SPACE), eq("창문이 열려 있습니다"),
                eq(Map.of("interlockId", "5", "interlockReason", "CONDITION_TRUE")));
        then(dispatcher).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("[ACT-07.01][AT-ACT-07.3][TC-ACT-124] 오프라인 기기에 같은 기능 명령 B → 대기 중 A SUPERSEDED, B QUEUED")
    void supersedeQueued() {
        shadow = new ShadowRow(Fixtures.AIRCON, Fixtures.ORG, DeviceShadow.EMPTY, null, null, "OFFLINE");
        Command a = Fixtures.command(UUID.randomUUID(), "Thermostat", Map.of("targetTemperature", 25), CommandPriority.MANUAL,
                CommandStatus.QUEUED, clock.instant());
        given(commands.lockPending(anyLong(), eq("Thermostat"), any())).willReturn(List.of(a));

        Outcome b = facade.submit(request(SourceType.USER, Map.of("targetTemperature", 24), null));

        assertThat(b.command().status()).isEqualTo(CommandStatus.QUEUED);
        then(events).should().transition(eq(a), org.mockito.ArgumentMatchers.argThat(c -> c.status() == CommandStatus.SUPERSEDED),
                any(), any());
    }

    @ParameterizedTest(name = "대기 {0}개 → {1}")
    @org.junit.jupiter.params.provider.CsvSource({"0, QUEUED", "9, QUEUED", "10, FAILED"})
    @DisplayName("[ACT-07.01][TC-ACT-125] BR-ACT-13 규칙 표: 오프라인 대기열은 기기당 10개(넘으면 FAILED(QUEUE_FULL))")
    void queueLimit(int alreadyQueued, CommandStatus expected) {
        shadow = new ShadowRow(Fixtures.AIRCON, Fixtures.ORG, DeviceShadow.EMPTY, null, null, "OFFLINE");
        given(commands.countQueued(Fixtures.AIRCON)).willReturn(alreadyQueued);

        Outcome o = facade.submit(request(SourceType.USER, Map.of("targetTemperature", 24), null));

        assertThat(o.command().status()).isEqualTo(expected);
        if (expected == CommandStatus.FAILED) {
            assertThat(o.command().statusReason()).isEqualTo(ControlFacade.QUEUE_FULL);
        }
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-128] LoRaWAN Class A 기기 명령 → QUEUED_FOR_DOWNLINK, 예상 시각 = 마지막 업링크 + 보고 주기")
    void classADownlinkQueued() {
        ModelCapability classA = new ModelCapability(Map.of(), null, false, true);
        ControlProfile p = new ControlProfile(Fixtures.AIRCON, Fixtures.ORG, Fixtures.SPACE, "밸브", "70b3d57ed0000001", false, "ACTIVE", 3L,
                Map.of("Switch", classA), new DriverBinding(11L, "LORAWAN", Map.of(), 30, 60, null), Fixtures.settings()).withReportInterval(600);
        given(profiles.find(Fixtures.AIRCON)).willReturn(Optional.of(p));
        online(DeviceShadow.EMPTY.withReported(1, Map.of("Switch", Map.of("on", false)), clock.instant().minusSeconds(240)).orElseThrow());
        CommandRequest req = new CommandRequest(Fixtures.ORG, Fixtures.AIRCON, "Switch", "set", Map.of("on", true), CommandSource.user(7),
                null, "k-a", null, null, null, false, false);

        Outcome o = facade.submit(req);

        assertThat(o.command().status()).isEqualTo(CommandStatus.QUEUED_FOR_DOWNLINK);
        assertThat(o.command().timeoutAt()).isEqualTo(o.command().validUntil());
        then(commands).should().saveExpectedDelivery(o.command().id(), clock.instant().plusSeconds(360));
        then(dispatcher).shouldHaveNoInteractions();
    }

    @Test
    @DisplayName("[ACT-05.01][BR-ACT-24] 장면 출처 명령: 실행 출처가 사용자면 MANUAL, 플로우면 AUTO")
    void sceneSourcePriority() {
        CommandSource userScene = net.java21.data2flow.action.actuation.domain.SceneRules.sceneSource(CommandSource.user(7), "41");
        Outcome o = facade.submit(new CommandRequest(Fixtures.ORG, Fixtures.AIRCON, "Thermostat", "set", Map.of("targetTemperature", 24),
                userScene, null, "k-s", null, null, null, false, false));
        assertThat(o.command().priority()).isEqualTo(CommandPriority.MANUAL);
        assertThat(ControlFacade.priorityOf(net.java21.data2flow.action.actuation.domain.SceneRules.sceneSource(
                CommandSource.flow("f", 1, "n", "m"), "42"))).isEqualTo(CommandPriority.AUTO);
    }

    @Test
    @DisplayName("[ACT-06.01][TC-ACT-098] 하루 반복 한도를 넘은 켜기 → BLOCKED(PROTECTION)")
    void protectionBlocked() {
        given(deviceState.findProtection(anyLong(), anyLong())).willReturn(new ProtectionState(null, null, 20,
                java.time.LocalDate.of(2026, 3, 2)));
        online(DeviceShadow.EMPTY.withReported(1, Map.of("Thermostat", Map.of("mode", "off")), clock.instant()).orElseThrow());

        Outcome o = facade.submit(request(SourceType.USER, Map.of("mode", "cool"), null));

        assertThat(o.command().statusReason()).isEqualTo("PROTECTION");
        assertThat(o.command().status()).isEqualTo(CommandStatus.BLOCKED);
    }

    @Test
    @DisplayName("[ACT-02.01] 없는 기기·다른 조직 → 404 DEVICE_NOT_FOUND, 드라이버 없는 기기 → 409 DEVICE_NOT_CONTROLLABLE, 장면 실행 ID 없는 장면 출처 → 400(기록 없음)")
    void notControllable() {
        given(profiles.find(99L)).willReturn(Optional.empty());
        CommandRequest missing = new CommandRequest(Fixtures.ORG, 99, "Switch", "set", Map.of("on", true), CommandSource.user(7), null, "a",
                null, null, null, false, false);
        assertThatThrownBy(() -> facade.submit(missing)).isInstanceOf(BusinessException.class).extracting("errorCode")
                .isEqualTo(ActionErrorCode.DEVICE_NOT_FOUND);

        ControlProfile noDriver = new ControlProfile(Fixtures.AIRCON, Fixtures.ORG, 31L, "x", "x", false, "ACTIVE", 1L,
                Map.of("Switch", ModelCapability.PLAIN), null, ControlSettings.DEFAULT);
        given(profiles.find(Fixtures.AIRCON)).willReturn(Optional.of(noDriver));
        assertThatThrownBy(() -> facade.submit(request(SourceType.USER, Map.of("on", true), null))).isInstanceOf(BusinessException.class)
                .extracting("errorCode").isEqualTo(ActionErrorCode.DEVICE_NOT_CONTROLLABLE);

        CommandRequest scene = new CommandRequest(Fixtures.ORG, Fixtures.AIRCON, "Switch", "set", Map.of("on", true),
                new CommandSource(SourceType.SCENE, null, null, null, null, null, null, null, null, null, null), null, "b", null, null,
                null, false, false);
        assertThatThrownBy(() -> facade.submit(scene)).isInstanceOf(BusinessException.class);
        then(commands).should(never()).insert(any(), anyString());
    }

    @Test
    @DisplayName("[ACT-02.04] 오프라인 기기 명령은 QUEUED(유효 시각까지), 보호 시간 안이면 DELAYED")
    void queuedAndDelayed() {
        shadow = new ShadowRow(Fixtures.AIRCON, Fixtures.ORG, DeviceShadow.EMPTY, null, null, "OFFLINE");
        Outcome queued = facade.submit(request(SourceType.USER, Map.of("targetTemperature", 24), null));
        assertThat(queued.command().status()).isEqualTo(CommandStatus.QUEUED);
        assertThat(queued.command().timeoutAt()).isEqualTo(clock.instant().plusSeconds(600));

        online(DeviceShadow.EMPTY.withReported(1, Map.of("Thermostat", Map.of("mode", "off")), clock.instant()).orElseThrow());
        given(deviceState.findProtection(anyLong(), anyLong())).willReturn(new ProtectionState(null, clock.instant().minusSeconds(60), 1,
                java.time.LocalDate.of(2026, 3, 2)));
        Outcome delayed = facade.submit(request(SourceType.USER, Map.of("mode", "cool"), null));
        assertThat(delayed.command().status()).isEqualTo(CommandStatus.DELAYED);
        assertThat(delayed.command().executeAfter()).isEqualTo(clock.instant().plusSeconds(120));
    }
}
