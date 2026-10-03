package net.java21.data2flow.action.actuation.controller;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.repository.CommandEventRepository;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.service.CommandQueryService;
import net.java21.data2flow.action.actuation.service.CommandRequest;
import net.java21.data2flow.action.actuation.service.CommandWaiter;
import net.java21.data2flow.action.actuation.service.ControlFacade;
import net.java21.data2flow.action.actuation.service.Outcome;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 내부 API 슬라이스(ACT-api §5.2): 공통 응답·오류 코드·HTTP 상태·거부 명령 ID. TC-ACT-019·033·054
 */
@WebMvcTest(InternalCommandController.class)
@Import(InternalCommandControllerWebTest.Beans.class)
class InternalCommandControllerWebTest {

    @TestConfiguration
    @EnableConfigurationProperties(ActionProperties.class)
    static class Beans {
        @Bean
        Clock clock() {
            return new MutableClock(MutableClock.T0);
        }
    }

    @Autowired
    MockMvc mvc;
    @MockitoBean
    ControlFacade facade;
    @MockitoBean
    CommandQueryService queries;
    @MockitoBean
    CommandRepository commands;
    @MockitoBean
    CommandEventRepository timeline;
    @MockitoBean
    CommandWaiter waiter;

    private static final String FLOW_BODY = """
            {"deviceId":"15","capability":"Thermostat","command":"set","args":{"mode":"cool"},
             "source":{"type":"FLOW","flowId":"f-1","flowVersion":13,"nodeId":"n-act-1"},"sourceSpaceId":"31","priority":"MANUAL"}""";

    private Command command(CommandStatus status, String reason, CommandPriority priority) {
        Command c = Fixtures.command(UUID.fromString("8f1c2d3e-0000-4000-8000-000000000001"), "Thermostat", Map.of("mode", "cool"),
                priority, status, MutableClock.T0);
        return new Command(c.id(), c.organizationId(), c.idempotencyKey(), c.deviceId(), c.capability(), c.command(), c.args(), c.priority(),
                c.source(), status, reason, c.validUntil(), null, 0, c.requestedAt(), null, null, null, null, null);
    }

    @Test
    @DisplayName("[ACT-02.01][AT-ACT-15.1][TC-ACT-019·033] 샌드박스 출처 + 실제 기기 → 403 ACT_SANDBOX_FORBIDDEN, response.commandId(REJECTED)")
    void sandboxForbidden() throws Exception {
        given(facade.submit(any())).willReturn(new Outcome(command(CommandStatus.REJECTED, "SANDBOX_FORBIDDEN", CommandPriority.AUTO), false,
                new Outcome.Rejection(ActionErrorCode.ACT_SANDBOX_FORBIDDEN, List.of(), new Object[0], null), null));

        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("X-CALLER-SERVICE", "data2flow-core-api")
                        .header("Idempotency-Key", "sbx-1").contentType(MediaType.APPLICATION_JSON).content(FLOW_BODY))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.header.isSuccessful").value(false))
                .andExpect(jsonPath("$.header.resultCode").value("ACT_SANDBOX_FORBIDDEN"))
                .andExpect(jsonPath("$.header.resultMessage").value("샌드박스에서는 실제 장비를 제어할 수 없습니다"))
                .andExpect(jsonPath("$.response.commandId").value("8f1c2d3e-0000-4000-8000-000000000001"))
                .andExpect(jsonPath("$.response.status").value("REJECTED"));
        ArgumentCaptor<CommandRequest> req = ArgumentCaptor.forClass(CommandRequest.class);
        then(facade).should().submit(req.capture());
        assertThat(req.getValue().sourceSpaceId()).isEqualTo(31L);
        assertThat(req.getValue().checkPermission()).isFalse();
        assertThat(req.getValue().requestedPriority()).isEqualTo(CommandPriority.MANUAL);   // 무시되고 감사에만(BR-ACT-24)
        assertThat(req.getValue().idempotencyKey()).hasSize(64);
    }

    @Test
    @DisplayName("[ACT-02.05][TC-ACT-054] AUTO 명령 최소 간격 위반 → 429 COMMAND_RATE_LIMITED, Retry-After, '{n}초 후에 다시 시도하세요'")
    void rateLimited() throws Exception {
        given(facade.submit(any())).willReturn(new Outcome(command(CommandStatus.REJECTED, "RATE_LIMITED", CommandPriority.AUTO), false,
                new Outcome.Rejection(ActionErrorCode.COMMAND_RATE_LIMITED, List.of(), new Object[]{7L}, Duration.ofSeconds(7)), null));

        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "r-1")
                        .contentType(MediaType.APPLICATION_JSON).content(FLOW_BODY))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("Retry-After", "7"))
                .andExpect(jsonPath("$.header.resultCode").value("COMMAND_RATE_LIMITED"))
                .andExpect(jsonPath("$.header.resultMessage").value("7초 후에 다시 시도하세요"));
    }

    @Test
    @DisplayName("[ACT-02.01][API-ACT-01] 사용자 명령은 202 + Command(출처 USER, 신원 헤더 사용자), wait=none 차단은 202(BLOCKED)")
    void accepted() throws Exception {
        given(facade.submit(any())).willReturn(new Outcome(command(CommandStatus.SENT, null, CommandPriority.MANUAL), false, null, null));
        given(timeline.findByCommand(anyLong(), any())).willReturn(List.of());

        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "u-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deviceId\":\"15\",\"capability\":\"Thermostat\",\"command\":\"set\",\"args\":{\"mode\":\"cool\"}}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.header.isSuccessful").value(true))
                .andExpect(jsonPath("$.response.id").value("8f1c2d3e-0000-4000-8000-000000000001"))
                .andExpect(jsonPath("$.response.deviceId").value("15"))
                .andExpect(jsonPath("$.response.source.type").value("USER"));
        ArgumentCaptor<CommandRequest> req = ArgumentCaptor.forClass(CommandRequest.class);
        then(facade).should().submit(req.capture());
        assertThat(req.getValue().checkPermission()).isTrue();
        assertThat(req.getValue().source().userId()).isEqualTo(7L);

        given(facade.submit(any())).willReturn(new Outcome(command(CommandStatus.BLOCKED, "OSCILLATION", CommandPriority.MANUAL), false,
                new Outcome.Rejection(ActionErrorCode.COMMAND_BLOCKED, List.of(), new Object[]{"OSCILLATION"}, null), "반대 명령이 반복되어 차단했습니다"));
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "u-2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deviceId\":\"15\",\"capability\":\"Thermostat\",\"command\":\"set\",\"args\":{\"mode\":\"cool\"}}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.response.status").value("BLOCKED"))
                .andExpect(jsonPath("$.response.message").value("반대 명령이 반복되어 차단했습니다"));
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "u-3")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deviceId\":\"15\",\"capability\":\"Thermostat\",\"command\":\"set\",\"args\":{\"mode\":\"cool\"},\"wait\":\"ack\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.header.resultCode").value("COMMAND_BLOCKED"))
                .andExpect(jsonPath("$.response.statusReason").value("OSCILLATION"));
    }

    @Test
    @DisplayName("[OPS-12.03] Idempotency-Key 없음·형식 오류 → 400 INVALID_REQUEST, 신원 없음 → 401, 숫자가 아닌 deviceId → 400")
    void validation() throws Exception {
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"deviceId\":\"15\",\"capability\":\"Switch\",\"command\":\"set\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("Idempotency-Key"));
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "bad key!")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"deviceId\":\"15\",\"capability\":\"Switch\",\"command\":\"set\"}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/internal/action/commands").header("Idempotency-Key", "k")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"deviceId\":\"15\",\"capability\":\"Switch\",\"command\":\"set\"}"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "k")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"deviceId\":\"ac\",\"capability\":\"Switch\",\"command\":\"set\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("deviceId"));
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "k")
                        .contentType(MediaType.APPLICATION_JSON).content("{\"deviceId\":\"15\",\"command\":\"set\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("capability"));
        mvc.perform(post("/internal/action/commands").header("X-USER-ID", "7").header("X-ORG-ID", "1").header("Idempotency-Key", "k")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"deviceId\":\"15\",\"capability\":\"Switch\",\"command\":\"set\",\"source\":{\"type\":\"USER\",\"userId\":\"8\"}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors[0].field").value("source.userId"));
    }

    @Test
    @DisplayName("[ACT-02.02][TC-ACT-088] 없는 명령 → 404 COMMAND_NOT_FOUND '명령을 찾을 수 없습니다', 다른 언어 문구(ADR-037)")
    void notFound() throws Exception {
        given(queries.get(anyLong(), any())).willThrow(new BusinessException(ActionErrorCode.COMMAND_NOT_FOUND));

        mvc.perform(get("/internal/action/commands/8f1c2d3e-0000-4000-8000-000000000009").header("X-USER-ID", "7").header("X-ORG-ID", "1"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.header.resultMessage").value("명령을 찾을 수 없습니다"));
        mvc.perform(get("/internal/action/commands/8f1c2d3e-0000-4000-8000-000000000009").header("X-USER-ID", "7").header("X-ORG-ID", "1")
                        .header("Accept-Language", "en"))
                .andExpect(jsonPath("$.header.resultMessage").value("Command not found"));
        org.mockito.BDDMockito.willThrow(new BusinessException(CommonErrorCode.PERMISSION_DENIED)).given(queries).get(anyLong(), any());
        mvc.perform(get("/internal/action/commands/8f1c2d3e-0000-4000-8000-000000000009").header("X-USER-ID", "7").header("X-ORG-ID", "1"))
                .andExpect(status().isForbidden());
    }
}
