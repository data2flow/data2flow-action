package net.java21.data2flow.action.actuation.event;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.action.actuation.service.SandboxRegistry;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.ConfigChangedMessage.EntityType;
import net.java21.data2flow.contracts.message.ConfigChangedMessage.Op;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 설정 변경 종류별 캐시 무효화 범위(ADR-043 열린 요청 ①): CAPABILITY·DRIVER·MODEL은 그 대상을 쓰는 기기 프로필만 지운다.
 */
class ConfigChangeListenerTest {

    private static final long AIRCON = 15;   // 드라이버 9, 모델 3, Thermostat·Switch
    private static final long LAMP = 16;     // 드라이버 10, 모델 4, Dimmer

    private final Clock clock = Clock.fixed(Instant.parse("2026-10-04T00:00:00Z"), ZoneOffset.UTC);
    private final CoreClient core = mock(CoreClient.class);
    private ControlProfileService profiles;
    private ConfigChangeListener listener;
    private net.java21.data2flow.action.actuation.service.InterlockService interlocks;
    private net.java21.data2flow.action.actuation.service.EmergencyStopRegistry emergency;

    @Test
    @DisplayName("[ACT-06.02] INTERLOCK 변경은 인터락 캐시를 지우고, DEVICE 변경은 그 기기 인터락만 다시 읽는다")
    void interlockInvalidation() {
        when(core.interlocks(AIRCON)).thenReturn(java.util.List.of());
        when(core.interlocks(LAMP)).thenReturn(java.util.List.of());
        interlocks.interlocks(AIRCON);
        interlocks.interlocks(LAMP);
        listener.apply(new ConfigChangedMessage(1, UUID.randomUUID(), EntityType.DEVICE, "15", 2, Op.UPSERT, "1", clock.instant()));
        interlocks.interlocks(AIRCON);
        interlocks.interlocks(LAMP);
        verify(core, times(2)).interlocks(AIRCON);
        verify(core, times(1)).interlocks(LAMP);
        listener.apply(new ConfigChangedMessage(1, UUID.randomUUID(), EntityType.INTERLOCK, "3", 2, Op.UPSERT, "1", clock.instant()));
        interlocks.interlocks(LAMP);
        verify(core, times(2)).interlocks(LAMP);
    }

    @Test
    @DisplayName("[ACT-06.03] EMERGENCY_STOP 변경은 파드마다 비상 정지 목록을 다시 읽는다(1초 안 반영, BR-ACT-12)")
    void emergencyReload() {
        when(core.activeEmergencyStops()).thenReturn(java.util.List.of());
        emergency.current();
        listener.apply(new ConfigChangedMessage(1, UUID.randomUUID(), EntityType.EMERGENCY_STOP, "1", 1, Op.UPSERT, "1", clock.instant()));
        verify(core, times(2)).activeEmergencyStops();
        when(core.activeEmergencyStops()).thenThrow(new IllegalStateException("core down"));
        listener.apply(new ConfigChangedMessage(1, UUID.randomUUID(), EntityType.EMERGENCY_STOP, "1", 1, Op.UPSERT, "1", clock.instant()));
        // 다시 읽지 못하면 이전 목록을 쓴다
        org.assertj.core.api.Assertions.assertThat(emergency.current()).isEmpty();
    }

    @BeforeEach
    void setUp() {
        ControlProfile lamp = new ControlProfile(LAMP, Fixtures.ORG, Fixtures.SPACE, "조명", "lamp-16", true, "ACTIVE", 4L,
                Map.of("Dimmer", ModelCapability.PLAIN), new DriverBinding(10L, "VIRTUAL", Map.of(), 30, 60, null), null);
        when(core.controlProfile(AIRCON)).thenReturn(Optional.of(Fixtures.virtualAircon()));
        when(core.controlProfile(LAMP)).thenReturn(Optional.of(lamp));
        profiles = new ControlProfileService(core, clock,
                new ActionProperties(null, null, null, null, null, null, null, null, null, null));
        interlocks = new net.java21.data2flow.action.actuation.service.InterlockService(core,
                mock(net.java21.data2flow.action.actuation.repository.ShadowRepository.class), profiles, clock, java.time.Duration.ofSeconds(30));
        emergency = new net.java21.data2flow.action.actuation.service.EmergencyStopRegistry(core, clock, java.time.Duration.ofSeconds(30));
        listener = new ConfigChangeListener(profiles, mock(SandboxRegistry.class), interlocks, emergency);
        profiles.find(AIRCON);
        profiles.find(LAMP);
        clearInvocations(core);
    }

    private void receive(EntityType type, String id) {
        listener.apply(new ConfigChangedMessage(1, UUID.randomUUID(), type, id, 2, Op.UPSERT, "1", clock.instant()));
        profiles.find(AIRCON);
        profiles.find(LAMP);
    }

    @Test
    @DisplayName("[ACT-03.05] DRIVER 변경은 그 드라이버에 연결된 기기 프로필만 다시 읽는다")
    void driverInvalidatesBoundDevicesOnly() {
        receive(EntityType.DRIVER, "9");
        verify(core, times(1)).controlProfile(AIRCON);
        verify(core, never()).controlProfile(LAMP);
    }

    @Test
    @DisplayName("[ACT-01.04] CAPABILITY 변경은 그 기능을 지원하는 기기 프로필만 다시 읽는다")
    void capabilityInvalidatesSupportingDevicesOnly() {
        receive(EntityType.CAPABILITY, "Dimmer");
        verify(core, never()).controlProfile(AIRCON);
        verify(core, times(1)).controlProfile(LAMP);
    }

    @Test
    @DisplayName("[DEV-03.03] MODEL 변경은 그 모델의 기기 프로필만, 숫자가 아닌 ID면 모두 다시 읽는다")
    void modelInvalidatesModelDevices() {
        receive(EntityType.MODEL, "4");
        verify(core, never()).controlProfile(AIRCON);
        verify(core, times(1)).controlProfile(LAMP);
        clearInvocations(core);
        receive(EntityType.DRIVER, "x");
        verify(core, times(2)).controlProfile(anyLong());
    }

    @Test
    @DisplayName("[ACT-01.01] 조직 제어 설정(SETTING)·모르는 종류는 모든 프로필을 다시 읽는다")
    void settingInvalidatesAll() {
        receive(EntityType.SETTING, "control");
        verify(core, times(2)).controlProfile(anyLong());
        clearInvocations(core);
        receive(EntityType.UNKNOWN, "1");
        verify(core, times(2)).controlProfile(anyLong());
    }
}
