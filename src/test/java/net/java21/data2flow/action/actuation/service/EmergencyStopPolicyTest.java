package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.EmergencyStop;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.authz.BuiltinRole;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

/** 비상 정지(ACT-06.03, BR-ACT-12) */
class EmergencyStopPolicyTest {

    private final MutableClock clock = MutableClock.atUtc("2026-03-02T00:00:00Z");
    private final CoreClient core = mock(CoreClient.class);
    private final EmergencyStopRegistry registry = new EmergencyStopRegistry(core, clock, Duration.ofSeconds(30));

    /** 캠퍼스(1) / 본관(7) / 실습실(31), 다른 건물(8) */
    private static final List<Long> LAB = List.of(1L, 7L, 31L);
    private static final List<Long> OTHER = List.of(1L, 8L, 44L);

    private void active(EmergencyStopChanged.Scope scope) {
        given(core.activeEmergencyStops()).willReturn(List.of(new EmergencyStop(9, Fixtures.ORG, scope, "점검", clock.instant())));
    }

    @ParameterizedTest(name = "{0} 명령, 실습실 → 막힘={1}")
    @CsvSource({"AUTO, true", "SCHEDULE, true", "AI, true", "MANUAL, false", "SAFETY, false"})
    @DisplayName("[ACT-06.03][TC-ACT-108] BR-ACT-12 규칙 표: 범위 안 AUTO·SCHEDULE·AI는 막고 MANUAL·SAFETY는 허용")
    void priorities(CommandPriority priority, boolean blocked) {
        active(EmergencyStopChanged.Scope.space(7));
        assertThat(registry.blocking(Fixtures.ORG, priority, LAB).isPresent()).isEqualTo(blocked);
    }

    @Test
    @DisplayName("[ACT-06.03][AT-ACT-09.1][AT-ACT-09.2][TC-ACT-109] 공간(하위 포함) 비상 정지: 범위 밖 공간·다른 조직은 정상, 조직 전체는 모두")
    void scopes() {
        active(EmergencyStopChanged.Scope.space(7));
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.AUTO, LAB)).isPresent();
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.AUTO, OTHER)).isEmpty();
        assertThat(registry.blocking(Fixtures.OTHER_ORG, CommandPriority.AUTO, LAB)).isEmpty();
        registry.invalidate();
        active(EmergencyStopChanged.Scope.organization());
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.SCHEDULE, OTHER)).isPresent();
        // 하위 미포함이면 그 공간만
        registry.invalidate();
        active(new EmergencyStopChanged.Scope(EmergencyStopChanged.Scope.Type.SPACE, 7L, false));
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.AUTO, LAB)).isEmpty();
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.AUTO, List.of(1L, 7L))).isPresent();
    }

    @Test
    @DisplayName("[ACT-06.03][TC-ACT-109] 시작 이벤트: 범위 안 대기 중 자동 명령은 CANCELLED, 범위 밖은 그대로. 해제하면 다시 허용")
    void startedCancelsPending() {
        CommandRepository commands = mock(CommandRepository.class);
        CommandEvents events = mock(CommandEvents.class);
        ControlProfileService profiles = mock(ControlProfileService.class);
        Command inLab = Fixtures.command(UUID.randomUUID(), "Thermostat", Map.of("mode", "cool"), CommandPriority.AUTO, CommandStatus.QUEUED,
                clock.instant());
        Command other = new Command(UUID.randomUUID(), Fixtures.ORG, "k-o", 16, "Switch", "set", Map.of("on", true), CommandPriority.AUTO,
                inLab.source(), CommandStatus.DELAYED, null, clock.instant().plusSeconds(600), null, 0, clock.instant(), null, null, null, null,
                null);
        given(commands.lockPendingAutomatic(Fixtures.ORG)).willReturn(List.of(inLab, other));
        given(profiles.find(Fixtures.AIRCON)).willReturn(Optional.of(profile(Fixtures.AIRCON, LAB)));
        given(profiles.find(16L)).willReturn(Optional.of(profile(16, OTHER)));
        given(core.activeEmergencyStops()).willReturn(List.of());
        EmergencyStopHandler handler = new EmergencyStopHandler(registry, commands, events, profiles,
                mock(PlatformTransactionManager.class), clock);
        EmergencyStopChanged started = new EmergencyStopChanged(9, EmergencyStopChanged.Scope.space(7), "점검", 7L, clock.instant());

        // TransactionTemplate에 가짜 트랜잭션 관리자 → 콜백은 그대로 실행된다
        handler.started(Fixtures.ORG, started);

        then(events).should().transition(eq(inLab), argThat(c -> c.status() == CommandStatus.CANCELLED
                && "EMERGENCY_STOP".equals(c.statusReason())), eq(31L), any());
        then(events).should(never()).transition(eq(other), any(), any(), any());
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.AUTO, LAB)).isPresent();
        handler.released(Fixtures.ORG, started);
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.AUTO, LAB)).isEmpty();
    }

    @Test
    @DisplayName("[ACT-06.03] core에서 목록을 읽지 못했고 이전 목록도 없으면 판정하지 않고 예외(자동 명령은 다시 시도)")
    void unreadable() {
        given(core.activeEmergencyStops()).willThrow(new IllegalStateException("core down"));
        assertThatThrownBy(() -> registry.blocking(Fixtures.ORG, CommandPriority.AUTO, LAB)).isInstanceOf(IllegalStateException.class);
        assertThat(registry.blocking(Fixtures.ORG, CommandPriority.MANUAL, LAB)).isEmpty();
    }

    @Test
    @DisplayName("[ACT-06.03][AT-ACT-09.3][TC-ACT-103] 해제 권한(EMERGENCY_RELEASE)은 ADMIN·INTEGRATOR만 — OPERATOR는 403(core가 판정)")
    void releasePermission() {
        assertThat(BuiltinRole.OPERATOR.permissions()).contains(Permission.EMERGENCY_STOP).doesNotContain(Permission.EMERGENCY_RELEASE);
        assertThat(BuiltinRole.ADMIN.permissions()).contains(Permission.EMERGENCY_RELEASE);
        assertThat(BuiltinRole.INTEGRATOR.permissions()).contains(Permission.EMERGENCY_RELEASE);
    }

    private static ControlProfile profile(long deviceId, List<Long> path) {
        ControlProfile p = Fixtures.aircon(deviceId, Fixtures.ORG, true, false);
        return new ControlProfile(p.deviceId(), p.organizationId(), path.get(path.size() - 1), p.name(), p.externalId(), p.virtual(),
                p.status(), p.modelId(), p.capabilities(), p.driver(), p.settings(), path, null, null);
    }
}
