package net.java21.data2flow.action.it;

import net.java21.data2flow.action.actuation.service.EmergencyStopRegistry;
import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPayload;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandTarget;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.messaging.MessageHeaders;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import net.java21.data2flow.contracts.message.event.EmergencyStopChanged;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 안전 장치 통합 시험: 비상 정지(ACT-06.03, TC-ACT-106), 인터락(ACT-06.02), 진동 차단 이벤트(EVT-ACT-08), 수동 우선 표시(ACT-06.05).
 * 화면 배너(SSE)는 core·web 몫이고, action은 이벤트를 받아 대기 중 자동 명령을 취소하고 새 자동 명령을 건너뛴다.
 */
class SafetyControlIT extends IntegrationTestSupport {

    @Autowired
    EmergencyStopRegistry emergency;

    private RestClient operator;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.profiles.put(16L, Fixtures.aircon(16, Fixtures.ORG, true, false));
        CORE.profiles.put(17L, Fixtures.aircon(17, Fixtures.ORG, true, false));
        CORE.roles.put(Fixtures.USER, "ADMIN");
        operator = api(Fixtures.USER, Fixtures.ORG);
        emergency.invalidate();
    }

    private void sendFlow(long deviceId, Map<String, Object> args, String trigger) {
        sendFlow(deviceId, "Thermostat", args, trigger);
    }

    private void sendFlow(long deviceId, String capability, Map<String, Object> args, String trigger) {
        String key = ActionIdempotencyKeys.flow("f-1", "n-1", trigger);
        ActionRequest req = ActionRequest.command(Fixtures.ORG, key, CommandSource.flow("f-1", 3, "n-1", trigger), null,
                new CommandPayload(CommandTarget.device(deviceId), capability, "set", args, false), clock);
        MessageProperties props = new MessageProperties();
        props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        MessageHeaders.of(req).forEach(props::setHeader);
        rabbit.send(MessagingNames.EXCHANGE_ACTIONS, req.routingKey(), new Message(CODEC.write(req), props));
    }

    private String statusOf(String trigger) {
        return jdbc.sql("SELECT status || coalesce(':' || status_reason, '') FROM data2flow_action.commands WHERE source->>'triggerMessageId' = :t")
                .param("t", trigger).query(String.class).optional().orElse("");
    }

    @Test
    @DisplayName("[ACT-06.03][AT-ACT-09.4][TC-ACT-106] 비상 정지 시작 → 대기 중 자동 명령 CANCELLED, 새 자동 명령 SKIPPED, 수동 명령은 적용, 해제 뒤 자동 허용")
    void emergencyStop() {
        // 기기 16은 오프라인: 플로우 명령이 대기열에
        publish(EventType.DEVICE_CONNECTIVITY_CHANGED, new DeviceConnectivityChanged(16, DeviceConnectivityChanged.Connectivity.ONLINE, DeviceConnectivityChanged.Connectivity.OFFLINE, clock.instant(), 300, 3));
        await().atMost(Duration.ofSeconds(20)).until(() -> count(
                "SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = 16 AND connectivity = 'OFFLINE'") == 1);
        sendFlow(16, Map.of("mode", "cool"), "e-1");
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("e-1").equals("QUEUED"));

        // core가 비상 정지를 시작: 목록(API-ACT-46)과 이벤트(EVT-ACT-03)
        Map<String, Object> stop = Map.of("emergencyStopId", "9", "organizationId", "1",
                "scope", Map.of("type", "SPACE", "spaceId", 31, "includeChildren", true), "reason", "점검", "startedAt", clock.instant().toString());
        CORE.routes.put("/internal/core/emergency-stops", r -> FakeCore.json(200, Map.of("header", Map.of("isSuccessful", true),
                "responses", List.of(stop), "totalCount", 1)));
        publish(EventType.CONTROL_EMERGENCY_STARTED, new EmergencyStopChanged(9, EmergencyStopChanged.Scope.space(31), "점검", 1L,
                clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("e-1").equals("CANCELLED:EMERGENCY_STOP"));

        sendFlow(Fixtures.AIRCON, Map.of("mode", "cool"), "e-2");
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("e-2").equals("SKIPPED:EMERGENCY_STOP"));
        assertThat(get(operator, "/internal/action/devices/15/control").response().path("emergencyStop").path("emergencyStopId").asString())
                .isEqualTo("9");

        Result manual = post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", Map.of("mode", "cool")), "m-1");
        assertThat(manual.status()).isEqualTo(202);
        String manualId = manual.response().path("id").asString();
        await().atMost(Duration.ofSeconds(20)).until(() -> status(manualId).equals("APPLIED"));

        CORE.routes.put("/internal/core/emergency-stops", r -> FakeCore.json(200, Map.of("header", Map.of("isSuccessful", true),
                "responses", List.of(), "totalCount", 0)));
        publish(EventType.CONTROL_EMERGENCY_RELEASED, new EmergencyStopChanged(9, EmergencyStopChanged.Scope.space(31), "점검", 1L,
                clock.instant()));
        await().atMost(Duration.ofSeconds(20)).until(() -> emergency.covering(Fixtures.ORG, List.of(31L)).isEmpty());
        clock.advanceBy(Duration.ofSeconds(11));
        sendFlow(17, Map.of("mode", "heat"), "e-3");
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("e-3").startsWith("APPLIED"));
    }

    @Test
    @DisplayName("[ACT-06.02][AT-ACT-08.2] 창문(기기 21) 열림 → 냉방 BLOCKED(INTERLOCK)·인터락 message, wait=ack면 409, 차단 기록 조회")
    void interlock() {
        CORE.routes.put("/internal/core/devices/15/interlocks", r -> FakeCore.json(200, Map.of("header", Map.of("isSuccessful", true),
                "responses", List.of(Map.of("interlockId", "5", "name", "창문 열림 냉난방 금지", "spaceId", "31", "includeChildren", true,
                        "condition", Map.of("kind", "state", "deviceId", 21, "capability", "Contact", "attribute", "open", "op", "==", "value", true),
                        "forbid", Map.of("capability", "Thermostat", "argsMatch", Map.of("mode", Map.of("in", List.of("cool", "heat")))),
                        "message", "창문이 열려 있어 냉난방을 켤 수 없습니다")), "totalCount", 1)));
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(21, 3, Map.of("Contact", Map.of("open", true)), clock.instant(), true));
        await().atMost(Duration.ofSeconds(20)).until(() -> count(
                "SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = 21 AND reported_version = 3") == 1);

        Result r = post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", Map.of("mode", "cool"), "wait", "ack"), "i-1");
        assertThat(r.status()).isEqualTo(409);
        assertThat(r.code()).isEqualTo("COMMAND_BLOCKED");
        assertThat(r.body().path("header").path("resultMessage").asString()).isEqualTo("창문이 열려 있어 냉난방을 켤 수 없습니다");
        assertThat(r.response().path("statusReason").asString()).isEqualTo("INTERLOCK");
        // 송풍은 금지 대상이 아니다
        Result fan = post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", Map.of("mode", "fan")), "i-2");
        assertThat(fan.status()).isEqualTo(202);

        Result blocks = get(operator, "/internal/action/interlocks/5/blocks");
        assertThat(blocks.response().path("responses")).hasSize(1);
        assertThat(blocks.response().path("responses").get(0).path("message").asString()).isEqualTo("창문이 열려 있어 냉난방을 켤 수 없습니다");
    }

    @Test
    @DisplayName("[ACT-02.05][AT-ACT-02.4][EVT-ACT-08] 60초 안 반대 플로우 명령 세 번째 → BLOCKED(OSCILLATION)와 control.oscillation.blocked 1건")
    void oscillationEvent() {
        listen("control.oscillation.blocked");
        // Switch는 보호 시간이 없어 켜고 끄기가 바로 적용된다
        sendFlow(Fixtures.AIRCON, "Switch", Map.of("on", true), "o-1");
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("o-1").equals("APPLIED"));
        clock.advanceBy(Duration.ofSeconds(11));
        sendFlow(Fixtures.AIRCON, "Switch", Map.of("on", false), "o-2");
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("o-2").equals("APPLIED"));
        clock.advanceBy(Duration.ofSeconds(11));
        sendFlow(Fixtures.AIRCON, "Switch", Map.of("on", true), "o-3");
        await().atMost(Duration.ofSeconds(20)).until(() -> statusOf("o-3").equals("BLOCKED:OSCILLATION"));

        await().atMost(Duration.ofSeconds(20)).until(() -> events("control.oscillation.blocked").size() == 1);
        var payload = events("control.oscillation.blocked").get(0).path("payload");
        assertThat(payload.path("deviceId").asLong()).isEqualTo(15);
        assertThat(payload.path("capability").asString()).isEqualTo("Switch");
        assertThat(payload.path("flips").asInt()).isEqualTo(3);
        assertThat(payload.path("source").path("flowId").asString()).isEqualTo("f-1");
    }

    @Test
    @DisplayName("[ACT-06.05][AT-ACT-10.1][TC-ACT-120] 수동 제어 직후 기기 제어 정보 → 수동 우선 남은 시간 30분")
    void manualOverrideRemaining() {
        Result r = post(operator, "/internal/action/commands", Map.of("deviceId", "15", "capability", "Thermostat", "command", "set",
                "args", Map.of("mode", "cool")), "mo-1");
        String id = r.response().path("id").asString();
        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("APPLIED"));
        var override = get(operator, "/internal/action/devices/15/control").response().path("manualOverride");
        assertThat(override.path("capability").asString()).isEqualTo("Thermostat");
        assertThat(override.path("remainingSeconds").asLong()).isEqualTo(1800);
        clock.advanceBy(Duration.ofMinutes(10));
        assertThat(get(operator, "/internal/action/devices/15/control").response().path("manualOverride").path("remainingSeconds").asLong())
                .isEqualTo(1200);
    }
}
