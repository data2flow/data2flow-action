package net.java21.data2flow.action.support;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.ControlSettings;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.actuation.domain.Protection;
import net.java21.data2flow.contracts.capability.AttributeConstraint;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 시험 데이터({@code CommandFixtures}·{@code DeviceFixtures.virtualAircon()}, ACT test-plan). 가상 강의실 에어컨: 모델 범위 18~30℃,
 * 조직 절대 한계 18~28℃, 압축기 보호(최소 꺼짐 3분·켜짐 5분·하루 20회).
 */
public final class Fixtures {

    public static final long ORG = 1;
    public static final long OTHER_ORG = 2;
    public static final long USER = 7;
    public static final long AIRCON = 15;
    public static final long SPACE = 31;

    private Fixtures() {
    }

    public static ModelCapability thermostat(boolean reapply) {
        return new ModelCapability(Map.of("targetTemperature", AttributeConstraint.range(18, 30),
                "mode", AttributeConstraint.oneOf(List.of("off", "cool", "heat", "fan", "auto"))), Protection.COMPRESSOR, reapply, false);
    }

    public static ControlSettings settings() {
        return new ControlSettings(Map.of("Thermostat", Map.of("targetTemperature", AttributeConstraint.range(18, 28))), 30, 10, null, 600,
                true);
    }

    /** 가상 에어컨(virtual 드라이버, Switch + Thermostat) */
    public static ControlProfile virtualAircon() {
        return aircon(AIRCON, ORG, true, false);
    }

    public static ControlProfile aircon(long deviceId, long org, boolean virtual, boolean reapply) {
        Map<String, ModelCapability> caps = new LinkedHashMap<>();
        caps.put("Thermostat", thermostat(reapply));
        caps.put("Switch", ModelCapability.PLAIN);
        return new ControlProfile(deviceId, org, SPACE, "실습실 에어컨", "aircon-" + deviceId, virtual, "ACTIVE", 3L, caps,
                new DriverBinding(9L, "VIRTUAL", Map.of(), 30, 60, null), settings());
    }

    public static Command command(UUID id, String capability, Map<String, Object> args, CommandPriority priority, CommandStatus status,
                                  Instant at) {
        return new Command(id, ORG, "k-" + id, AIRCON, capability, "set", args, priority,
                priority == CommandPriority.MANUAL ? CommandSource.user(USER) : CommandSource.flow("f-1", 1, "n-1", "m-1"), status, null,
                at.plusSeconds(600), null, 0, at, null, null, null, null, at);
    }
}
