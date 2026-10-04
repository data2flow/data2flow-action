package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.Interlock;
import net.java21.data2flow.action.actuation.domain.InterlockEvaluator;
import net.java21.data2flow.action.actuation.domain.SceneDefinition;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.SceneBulkRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.authz.RoleChecker;
import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.error.BusinessException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 일괄 제어(ACT-02.06)·장면(ACT-05.01·05.03) */
class BulkAndSceneTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final CoreClient core = mock(CoreClient.class);
    private final ControlProfileService profiles = mock(ControlProfileService.class);
    private final ShadowRepository shadows = mock(ShadowRepository.class);
    private final SceneBulkRepository repository = mock(SceneBulkRepository.class);
    private final CommandRepository commands = mock(CommandRepository.class);
    private final ControlFacade facade = mock(ControlFacade.class);
    private final InterlockService interlocks = mock(InterlockService.class);
    private BulkControlService bulk;
    private SceneService scenes;

    /** 수업 모드: 에어컨(15) 냉방 24, 환기(16) 2단, 조명(17) 100% */
    private static final SceneDefinition CLASS_MODE = new SceneDefinition(3, Fixtures.ORG, "수업 모드", 31L, List.of(
            new SceneDefinition.Item(new SceneDefinition.Target(15L, null, null, null, null), "Thermostat", Map.of("mode", "cool",
                    "targetTemperature", 24)),
            new SceneDefinition.Item(new SceneDefinition.Target(16L, null, null, null, null), "Ventilation", Map.of("mode", "on", "level", 2)),
            new SceneDefinition.Item(new SceneDefinition.Target(17L, null, null, null, null), "Dimmer", Map.of("level", 100))));

    @BeforeEach
    void setUp() {
        bulk = new BulkControlService(core, profiles, shadows, repository, commands, facade, mock(RoleChecker.class), Runnable::run, clock);
        scenes = new SceneService(core, profiles, shadows, interlocks, repository, commands, facade, clock);
        given(profiles.find(anyLong())).willAnswer(inv -> Optional.of(Fixtures.aircon(inv.getArgument(0), Fixtures.ORG, true, false)));
        given(interlocks.evaluate(any(), any(), anyString(), anyString(), any())).willReturn(InterlockEvaluator.Decision.PASS);
        given(shadows.findByDeviceIdAndOrganizationId(anyLong(), anyLong())).willReturn(Optional.empty());
    }

    @Test
    @DisplayName("[ACT-02.06][AT-ACT-04.2][TC-ACT-056] 일괄 대상 500대 초과 → COMMAND_BULK_LIMIT_EXCEEDED")
    void bulkLimit() {
        given(core.spaceDevices(eq(31L), anyString(), anyString(), anyBoolean())).willReturn(LongStream.rangeClosed(1, 501).boxed().toList());
        var req = new BulkControlService.BulkRequest(List.of(), 31L, true, "Switch", "set", Map.of("on", true));
        assertThatThrownBy(() -> bulk.start(Fixtures.ORG, Fixtures.USER, req)).isInstanceOf(BusinessException.class)
                .extracting("errorCode").isEqualTo(ActionErrorCode.COMMAND_BULK_LIMIT_EXCEEDED);
    }

    @Test
    @DisplayName("[ACT-02.06][TC-ACT-058] 기능을 지원하지 않는 기기가 섞이면 CAPABILITY_NOT_SUPPORTED(미지원 기기 목록), 대상이 없으면 400")
    void bulkUnsupported() {
        var req = new BulkControlService.BulkRequest(List.of(15L), null, false, "Lock", "set", Map.of("locked", true));
        assertThatThrownBy(() -> bulk.preview(Fixtures.ORG, req)).isInstanceOf(BusinessException.class)
                .extracting("errorCode").isEqualTo(ActionErrorCode.CAPABILITY_NOT_SUPPORTED);
        var empty = new BulkControlService.BulkRequest(List.of(), null, false, "Switch", "set", Map.of("on", true));
        assertThatThrownBy(() -> bulk.preview(Fixtures.ORG, empty)).isInstanceOf(BusinessException.class);
    }

    @Test
    @DisplayName("[ACT-02.06][AT-ACT-04.1] 일괄 실행: 기기마다 출처 BULK(작업 ID)로 창구에 내고 끝나면 집계(적용·대기)")
    void bulkStart() {
        given(repository.insertBulkJob(anyLong(), anyLong(), any(), anyString(), anyString(), any(), eq(2), any())).willReturn(77L);
        given(commands.findBySource(Fixtures.ORG, "BULK", "77")).willReturn(List.of(
                command(15, CommandStatus.APPLIED), command(16, CommandStatus.QUEUED)));

        Map<String, Object> r = bulk.start(Fixtures.ORG, Fixtures.USER, new BulkControlService.BulkRequest(List.of(15L, 16L), null, false,
                "Switch", "set", Map.of("on", true)));

        assertThat(r).containsEntry("bulkJobId", "77").containsEntry("total", 2);
        verify(facade, times(2)).submit(any());
        verify(repository).finishBulkJob(Fixtures.ORG, 77L, 1, 0, 1, clock.instant());
        assertThat(BulkControlService.tally(List.of(command(1, CommandStatus.SKIPPED), command(2, CommandStatus.BLOCKED))))
                .containsEntry("skipped", 1).containsEntry("failed", 1);
    }

    @Test
    @DisplayName("[ACT-05.01][AT-ACT-05.2][TC-ACT-090] 장면 항목 101개 → SCENE_ITEM_LIMIT_EXCEEDED, 다른 조직 장면 → SCENE_NOT_FOUND")
    void sceneItemLimit() {
        List<SceneDefinition.Item> items = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            items.add(new SceneDefinition.Item(new SceneDefinition.Target((long) i, null, null, null, null), "Switch", Map.of("on", true)));
        }
        given(core.scene(4L)).willReturn(Optional.of(new SceneDefinition(4, Fixtures.ORG, "많음", null, items)));
        assertThatThrownBy(() -> scenes.load(Fixtures.ORG, 4)).isInstanceOf(BusinessException.class)
                .extracting("errorCode").isEqualTo(ActionErrorCode.SCENE_ITEM_LIMIT_EXCEEDED);
        given(core.scene(5L)).willReturn(Optional.of(new SceneDefinition(5, Fixtures.OTHER_ORG, "남의 것", null, List.of())));
        assertThatThrownBy(() -> scenes.load(Fixtures.ORG, 5)).isInstanceOf(BusinessException.class)
                .extracting("errorCode").isEqualTo(ActionErrorCode.SCENE_NOT_FOUND);
    }

    @Test
    @DisplayName("[ACT-05.01][TC-ACT-092] BR-ACT-16: 관계 대상 항목은 실행 때 기기별로 펼친다")
    void expand() {
        given(core.spaceDevices(31L, "controls", "Dimmer", true)).willReturn(List.of(17L, 18L));
        SceneDefinition s = new SceneDefinition(6, Fixtures.ORG, "조명 끄기", 31L, List.of(
                new SceneDefinition.Item(new SceneDefinition.Target(null, 31L, "controls", "Dimmer", true), "Dimmer", Map.of("level", 0)),
                new SceneDefinition.Item(new SceneDefinition.Target(15L, null, null, null, null), "Switch", Map.of("on", false)),
                new SceneDefinition.Item(null, "Switch", Map.of())));
        assertThat(scenes.expand(s)).extracting(SceneService.Target::deviceId).containsExactly(17L, 18L, 15L);
    }

    @Test
    @DisplayName("[ACT-05.03][AT-ACT-05.1][TC-ACT-094] 미리보기: 조명이 이미 100%면 변경 없음, 에어컨·환기는 바뀜, 인터락에 걸릴 항목 표시")
    void preview() {
        given(core.scene(3L)).willReturn(Optional.of(CLASS_MODE));
        given(shadows.findByDeviceIdAndOrganizationId(17L, Fixtures.ORG)).willReturn(Optional.of(new ShadowRepository.ShadowRow(17L, Fixtures.ORG,
                DeviceShadow.EMPTY.withReported(1, Map.of("Dimmer", Map.of("level", 100)), clock.instant()).orElseThrow(), null, null,
                "ONLINE")));
        Interlock window = new Interlock(5L, "창문", 31L, true, null, null, "창문이 열려 있습니다", null);
        given(interlocks.evaluate(argProfile(15L), any(), eq("Thermostat"), anyString(), any()))
                .willReturn(new InterlockEvaluator.Decision(window, InterlockEvaluator.Reason.CONDITION_TRUE));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) scenes.preview(Fixtures.ORG, 3).get("items");

        assertThat(items).extracting(m -> m.get("willChange")).containsExactly(true, true, false);
        assertThat(items.get(0)).containsKey("predictedBlock");
        assertThat(items.get(2)).doesNotContainKey("predictedBlock");
    }

    @Test
    @DisplayName("[ACT-05.02][AT-ACT-05.3][TC-ACT-093] 실행: 에어컨은 인터락 BLOCKED, 나머지 적용 → 부분 성공(PARTIAL), 우선순위는 실행 출처(MANUAL)")
    void runPartial() {
        given(core.scene(3L)).willReturn(Optional.of(CLASS_MODE));
        given(repository.insertSceneRun(anyLong(), anyLong(), any(), any())).willReturn(41L);
        Command blocked = command(15, CommandStatus.BLOCKED);
        Command applied1 = command(16, CommandStatus.APPLIED);
        Command applied2 = command(17, CommandStatus.SKIPPED);
        given(facade.submit(any())).willAnswer(inv -> {
            CommandRequest r = inv.getArgument(0);
            assertThat(r.source().sceneRunId()).isEqualTo("41");
            assertThat(ControlFacade.priorityOf(r.source())).isEqualTo(CommandPriority.MANUAL);
            Command c = r.deviceId() == 15 ? blocked : r.deviceId() == 16 ? applied1 : applied2;
            return new Outcome(c, false, null, null);
        });
        given(commands.findBySource(Fixtures.ORG, "SCENE", "41")).willReturn(List.of(blocked, applied1, applied2));
        List<Map<String, Object>> saved = new ArrayList<>();
        given(repository.findSceneRun(Fixtures.ORG, 41L)).willAnswer(inv -> Optional.of(new SceneBulkRepository.SceneRun(41, 3, Map.of(), "RUNNING",
                saved, clock.instant(), null)));
        org.mockito.Mockito.doAnswer(inv -> {
            saved.clear();
            saved.addAll(inv.getArgument(3));
            return null;
        }).when(repository).saveSceneRun(eq(Fixtures.ORG), eq(41L), eq("RUNNING"), any(), any());

        long runId = scenes.run(Fixtures.ORG, 3, CommandSource.user(Fixtures.USER), "run-1");

        assertThat(runId).isEqualTo(41L);
        Map<String, Object> state = scenes.refresh(Fixtures.ORG, 41);
        assertThat(state).containsEntry("status", "PARTIAL");
        verify(repository, org.mockito.Mockito.atLeastOnce()).saveSceneRun(eq(Fixtures.ORG), eq(41L), eq("PARTIAL"), any(), any());
    }

    @ParameterizedTest(name = "reported {0}, 목표 {1} → 변경 없음={2}")
    @CsvSource({"24, 24, true", "25, 24, false"})
    @DisplayName("[ACT-05.03][TC-ACT-095] BR-ACT-04 규칙 표: 현재 상태가 이미 목표와 같으면 변경 없음")
    void noChange(int reported, int target, boolean same) {
        DeviceShadow s = DeviceShadow.EMPTY.withDesired("Thermostat", Map.of("targetTemperature", reported))
                .withReported(1, Map.of("Thermostat", Map.of("targetTemperature", reported)), clock.instant()).orElseThrow();
        assertThat(s.noChange("Thermostat", Map.of("targetTemperature", target))).isEqualTo(same);
    }

    private static ControlProfile argProfile(long deviceId) {
        return org.mockito.ArgumentMatchers.argThat(p -> p != null && p.deviceId() == deviceId);
    }

    private Command command(long deviceId, CommandStatus status) {
        return new Command(UUID.randomUUID(), Fixtures.ORG, "k-" + deviceId, deviceId, "Switch", "set", Map.of("on", true),
                CommandPriority.MANUAL, CommandSource.user(Fixtures.USER), status, null, clock.instant().plusSeconds(600), null, 0,
                clock.instant(), null, null, null, null, null);
    }
}
