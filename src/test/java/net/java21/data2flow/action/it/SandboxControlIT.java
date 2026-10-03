package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPayload;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandTarget;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 샌드박스(SIM-07.03, BR-ACT-23): 샌드박스 공간의 플로우는 가상 기기만 제어한다. 목록은 core에서 읽고 {@code data2flow.config}
 * SIM_SANDBOX를 받으면 1초 안에 다시 읽는다. TC-ACT-018·020·030
 */
class SandboxControlIT extends IntegrationTestSupport {

    private static final long REAL_AIRCON = 21;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.profiles.put(REAL_AIRCON, Fixtures.aircon(REAL_AIRCON, Fixtures.ORG, false, false));
        CORE.sandbox.add(Fixtures.SPACE);
    }

    private void flow(long deviceId, String trigger) {
        CORE.spaceDevices.put(Fixtures.SPACE, List.of(deviceId));
        ActionRequest req = ActionRequest.command(Fixtures.ORG, ActionIdempotencyKeys.flow("f-sbx", "n-1", trigger),
                CommandSource.flow("f-sbx", 1, "n-1", trigger), null,
                new CommandPayload(CommandTarget.space(Fixtures.SPACE, "controls", "Switch", false), "Switch", "set", Map.of("on", true), true),
                clock);
        MessageProperties props = new MessageProperties();
        MessageHeaders.of(req).forEach(props::setHeader);
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, req.routingKey(), new Message(CODEC.write(req), props));
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-15.1][TC-ACT-018] 샌드박스 공간 플로우 → 실제 에어컨 REJECTED(SANDBOX_FORBIDDEN), 드라이버 0회")
    void realDeviceRejected() {
        flow(REAL_AIRCON, "m-1");

        await().atMost(Duration.ofSeconds(10)).until(() -> count("SELECT count(*) FROM data2flow_action.commands WHERE status = 'REJECTED'") == 1);
        assertThat(jdbc.sql("SELECT status_reason FROM data2flow_action.commands").query(String.class).single()).isEqualTo("SANDBOX_FORBIDDEN");
        assertThat(SIM.received).isEmpty();
        assertThat(events("command.status.rejected")).hasSize(1);
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-15.2][TC-ACT-020] 같은 플로우가 가상 에어컨을 대상으로 하면 정상 진행 → APPLIED")
    void virtualDeviceAllowed() {
        flow(Fixtures.AIRCON, "m-2");

        await().atMost(Duration.ofSeconds(10)).until(() -> count("SELECT count(*) FROM data2flow_action.commands WHERE status = 'APPLIED'") == 1);
    }

    @Test
    @DisplayName("[SIM-07.03][AT-ACT-15.3][TC-ACT-030] sim.sandbox 해제 수신 1초 안 목록 갱신 → 같은 명령이 실제 기기에 허용")
    void sandboxReleaseApplied() {
        assertThat(sandbox.spaces()).contains(Fixtures.SPACE);
        CORE.sandbox.clear();
        ConfigChangedMessage change = ConfigChangedMessage.delete(ConfigChangedMessage.EntityType.SIM_SANDBOX, Fixtures.SPACE, 2,
                Fixtures.ORG, clock);
        rabbit.send(MessagingNames.EXCHANGE_CONFIG, "", new Message(CODEC.write(change), new MessageProperties()));

        await().atMost(Duration.ofSeconds(1)).until(() -> !sandbox.spaces().contains(Fixtures.SPACE));
        flow(REAL_AIRCON, "m-3");
        await().atMost(Duration.ofSeconds(10)).until(() -> count("SELECT count(*) FROM data2flow_action.commands WHERE status = 'APPLIED'") == 1);
        // 재시작(캐시 비움) 뒤에도 마지막 설정을 core에서 다시 읽는다
        sandbox.invalidate();
        assertThat(sandbox.spaces()).isEmpty();
    }

    @Test
    @DisplayName("[ACT-01.01] 기기 설정 변경(config DEVICE) 수신 → 그 기기의 제어 프로필을 다시 읽는다")
    void deviceConfigInvalidatesProfile() {
        profiles.find(Fixtures.AIRCON);
        int before = CORE.profileCalls.get();
        ConfigChangedMessage change = ConfigChangedMessage.upsert(ConfigChangedMessage.EntityType.DEVICE, Fixtures.AIRCON, 3, Fixtures.ORG, clock);
        rabbit.send(MessagingNames.EXCHANGE_CONFIG, "", new Message(CODEC.write(change), new MessageProperties()));

        await().atMost(Duration.ofSeconds(1)).until(() -> {
            profiles.find(Fixtures.AIRCON);
            return CORE.profileCalls.get() > before;
        });
    }
}
