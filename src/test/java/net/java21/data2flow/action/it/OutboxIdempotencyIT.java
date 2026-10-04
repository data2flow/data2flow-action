package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPayload;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandTarget;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 플로우 제어 노드의 행동 요청({@code data2flow.actions} · {@code command} → {@code action.commands}, EVT-FLW-05).
 * ACT-02.01·02.03, BR-ACT-02, reliability-and-ha.md ⑧
 */
class OutboxIdempotencyIT extends IntegrationTestSupport {

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.profiles.put(16L, Fixtures.aircon(16, Fixtures.ORG, true, false));
    }

    private void send(ActionRequest req) {
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        MessageHeaders.of(req).forEach(props::setHeader);
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, req.routingKey(), new Message(CODEC.write(req), props));
    }

    private ActionRequest flowCommand(CommandTarget target, String trigger) {
        String key = ActionIdempotencyKeys.flow("f-7f3a", "n-act-1", trigger);
        return ActionRequest.command(Fixtures.ORG, key, CommandSource.flow("f-7f3a", 13, "n-act-1", trigger), null,
                new CommandPayload(target, "Thermostat", "set", Map.of("mode", "cool", "targetTemperature", 24), true), clock);
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-02.2][TC-ACT-026] 같은 ActionRequest가 3번 → executed_actions 1행, 드라이버 1회, 결과 다시 알림")
    void executedOnce() {
        ActionRequest req = flowCommand(CommandTarget.device(Fixtures.AIRCON), "m-1");
        send(req);
        send(req);
        send(ActionRequest.command(Fixtures.ORG, req.idempotencyKey(), req.source(), null, req.commandPayload(), clock));   // 새 messageId

        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.processed_messages") == 2
                && count("SELECT count(*) FROM data2flow_action.executed_actions") == 1);
        // 처음 처리의 REQUESTED·SENT·ACKED·APPLIED 4건 + 재요청 2번의 결과 다시 알림 2건
        await().atMost(Duration.ofSeconds(20)).until(() -> events(null).stream()
                .filter(e -> e.path("type").asString().startsWith("command.status.")).count() == 6);
        assertThat(count("SELECT count(*) FROM data2flow_action.commands")).isEqualTo(1);
        assertThat(SIM.commandsFor(Fixtures.AIRCON)).isEqualTo(1);
        // 우선순위는 출처가 정한다(AUTO), 출처 기록
        assertThat(jdbc.sql("SELECT priority FROM data2flow_action.commands").query(String.class).single()).isEqualTo("AUTO");
        assertThat(jdbc.sql("SELECT source->>'nodeId' FROM data2flow_action.commands").query(String.class).single()).isEqualTo("n-act-1");
        assertThat(events("command.status.applied").get(0).path("payload").path("idempotencyKey").asString()).isEqualTo(req.idempotencyKey());
    }

    @Test
    @DisplayName("[ACT-02.03][AT-ACT-02.3][TC-ACT-039] 관계 대상(공간 31의 controls Thermostat) 에어컨 2대 → 기기별 명령 2건")
    void relationTargetExpanded() {
        CORE.spaceDevices.put(Fixtures.SPACE, List.of(Fixtures.AIRCON, 16L));
        send(flowCommand(CommandTarget.space(Fixtures.SPACE, "controls", "Thermostat", false), "m-2"));

        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.commands WHERE status = 'APPLIED'") == 2);
        assertThat(jdbc.sql("SELECT result_ref FROM data2flow_action.executed_actions").query(String.class).single()).isEqualTo("n=2");
    }

    @Test
    @DisplayName("[ACT-02.01] 유효 시각이 지난 요청은 실행하지 않고 FAILED(EXPIRED), 읽을 수 없는 메시지는 DLQ")
    void expiredAndMalformed() {
        ActionRequest base = flowCommand(CommandTarget.device(Fixtures.AIRCON), "m-3");
        send(ActionRequest.command(Fixtures.ORG, base.idempotencyKey(), base.source(), clock.instant().minusSeconds(1),
                base.commandPayload(), clock));
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, "command", new Message("{\"v\":9}".getBytes(StandardCharsets.UTF_8), new MessageProperties()));

        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.commands WHERE status = 'FAILED'") == 1);
        assertThat(jdbc.sql("SELECT status_reason FROM data2flow_action.commands").query(String.class).single()).isEqualTo("EXPIRED");
        assertThat(SIM.received).isEmpty();
        await().atMost(Duration.ofSeconds(20)).until(() -> rabbit.receive("action.commands.dlq", 100) != null);
    }

    @Test
    @DisplayName("[ACT-02.03][TC-ACT-040] 승인자 없는 AI 출처 행동 요청은 실행하지 않고 DLQ(BR-ACT-15)")
    void aiWithoutApprovalGoesToDlq() {
        ActionRequest req = ActionRequest.command(Fixtures.ORG, ActionIdempotencyKeys.of("ai", "s-1"),
                new CommandSource(net.java21.data2flow.contracts.command.SourceType.AI, null, null, null, null, null, null, null, null, "s-1", null),
                null, new CommandPayload(CommandTarget.device(Fixtures.AIRCON), "Switch", "set", Map.of("on", true), false), clock);
        send(req);

        await().atMost(Duration.ofSeconds(20)).until(() -> rabbit.receive("action.commands.dlq", 100) != null);
        assertThat(count("SELECT count(*) FROM data2flow_action.commands")).isZero();
        assertThat(SIM.received).isEmpty();
    }
}
