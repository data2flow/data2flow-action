package net.java21.data2flow.action.actuation.event;

import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.capability.StateChange;
import net.java21.data2flow.contracts.command.CommandPayload;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageSchemas;
import net.java21.data2flow.contracts.message.event.CommandStatusChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateChanged;
import net.java21.data2flow.contracts.test.message.MessageFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 메시지 계약(TC-ACT-027): flow-engine이 만드는 ActionRequest 골든 픽스처를 action이 손실 없이 읽고, action이 내는 EVT-ACT-01·02가
 * 스키마를 통과한다(생산자 쪽 FlowEngineActionRequestContractTest와 같은 픽스처).
 */
class ActionRequestContractTest {

    private final MessageCodec codec = MessageCodec.create();
    private final MutableClock clock = new MutableClock(MutableClock.T0);

    @Test
    @DisplayName("[ACT-02.01][TC-ACT-027] 플로우 '고온이면 냉방' 행동 요청 픽스처: 스키마 통과, COMMAND 본문(관계 대상) 역직렬화")
    void flowFixture() {
        ActionRequest req = codec.read(MessageFixtures.actionRequestJson("flow-command-heatwave"), ActionRequest.class);
        MessageSchemas.assertValid(req);

        CommandPayload p = req.commandPayload();
        assertThat(p.target().spaceId()).isEqualTo(31);
        assertThat(p.target().relation()).isEqualTo("controls");
        assertThat(p.args()).containsEntry("mode", "cool");
        assertThat(req.source().nodeId()).isEqualTo("n-act-1");
        assertThat(req.priority()).isEqualTo(CommandPriority.AUTO);
    }

    @Test
    @DisplayName("[ACT-02.01][TC-ACT-027] 모르는 필드가 섞인 사용자 명령 픽스처도 읽는다(호환 규칙)")
    void unknownFields() {
        ActionRequest req = MessageFixtures.actionRequest("user-command-device");
        assertThat(req.commandPayload().target().deviceId()).isEqualTo(15);
        assertThat(req.source().userId()).isEqualTo(7);
    }

    @Test
    @DisplayName("[ACT-02.02][EVT-ACT-01·02] action이 내는 command.status.*·device.state.changed가 domain-event 스키마를 통과한다")
    void producedEvents() {
        DomainEvent<CommandStatusChanged> status = DomainEvent.of(EventType.commandStatus(CommandStatus.APPLIED), 1,
                new CommandStatusChanged(UUID.randomUUID(), "k", 15, 31L, "Thermostat", "set", Map.of("mode", "cool"), CommandStatus.APPLIED,
                        null, null, CommandSource.flow("f-1", 13, "n-act-1", "m-1"), CommandPriority.AUTO, clock.instant()), null, clock);
        DomainEvent<DeviceStateChanged> state = DomainEvent.of(EventType.DEVICE_STATE_CHANGED, 1,
                new DeviceStateChanged(15, 31L, DeviceStateChanged.Connectivity.ONLINE, Map.of("Thermostat", Map.of("mode", "cool")),
                        List.of(new StateChange("Thermostat", "mode", "off", "cool")), 8, Map.of(), clock.instant(),
                        DeviceStateChanged.Origin.COMMAND), null, clock);

        MessageSchemas.assertValid(status);
        MessageSchemas.assertValid(state);
        assertThat(status.type()).isEqualTo("command.status.applied");
    }

    @Test
    @DisplayName("[ACT-02.02][EVT-SIM-03] 시뮬레이터 응답 픽스처(device.command.ack·device.state.reported)를 읽는다")
    void simulatorFixtures() {
        DomainEvent<?> ack = MessageFixtures.domainEvent("device-command-ack-virtual");
        DomainEvent<?> reported = MessageFixtures.domainEvent("device-state-reported-virtual");
        assertThat(ack.type()).isEqualTo("device.command.ack");
        assertThat(reported.type()).isEqualTo("device.state.reported");
    }
}
