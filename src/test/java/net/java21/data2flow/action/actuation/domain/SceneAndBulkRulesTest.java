package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.SourceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

/** 일괄(BR-ACT-17)·장면(BR-ACT-16, BR-ACT-24)·Class A(ACT-07.02) 규칙 */
class SceneAndBulkRulesTest {

    @Test
    @DisplayName("[ACT-02.06][TC-ACT-058] BR-ACT-17 규칙 표: 500대 이하 + 모두 기능 지원이어야 한다")
    void bulkPolicy() {
        List<Long> ok = LongStream.rangeClosed(1, 500).boxed().toList();
        Map<Long, Boolean> all = new java.util.HashMap<>();
        ok.forEach(d -> all.put(d, true));
        assertThat(BulkPolicy.check(ok, all).ok()).isTrue();
        List<Long> over = LongStream.rangeClosed(1, 501).boxed().toList();
        assertThat(BulkPolicy.check(over, all).code()).isEqualTo("COMMAND_BULK_LIMIT_EXCEEDED");
        all.put(7L, false);
        var c = BulkPolicy.check(ok, all);
        assertThat(c.ok()).isFalse();
        assertThat(c.code()).isEqualTo("CAPABILITY_NOT_SUPPORTED");
        assertThat(c.unsupported()).containsExactly(7L);
        assertThat(BulkPolicy.check(List.of(), all).code()).isEqualTo("COMMAND_BULK_EMPTY");
    }

    @ParameterizedTest(name = "장면 실행 출처 {0} → 우선순위 {1}")
    @CsvSource({"USER, MANUAL", "SCHEDULE, SCHEDULE", "FLOW, AUTO", "AI, AI", "SYSTEM, SAFETY"})
    @DisplayName("[ACT-05.01][TC-ACT-092] BR-ACT-24: 장면 명령의 우선순위는 실행 출처를 따른다")
    void scenePriority(SourceType origin, CommandPriority expected) {
        CommandSource run = switch (origin) {
            case USER -> CommandSource.user(7);
            case SCHEDULE -> CommandSource.schedule(3);
            case FLOW -> CommandSource.flow("f-1", 1, "n-1", "m-1");
            case AI -> CommandSource.ai("s-1", 7);
            default -> CommandSource.system();
        };
        CommandSource scene = SceneRules.sceneSource(run, "41");
        assertThat(scene.type()).isEqualTo(SourceType.SCENE);
        assertThat(scene.sceneRunId()).isEqualTo("41");
        assertThat(SceneRules.origin(scene)).isEqualTo(origin);
        assertThat(CommandPriority.forScene(SceneRules.origin(scene))).isEqualTo(expected);
        assertThat(scene.requireComplete()).isEqualTo(scene);
    }

    @Test
    @DisplayName("[ACT-05.01][TC-ACT-092] BR-ACT-16: 일부 실패해도 나머지는 적용(PARTIAL), 진행 중이면 RUNNING")
    void sceneStatus() {
        assertThat(SceneRules.status(List.of(CommandStatus.APPLIED, CommandStatus.SKIPPED))).isEqualTo(SceneRules.RunStatus.SUCCEEDED);
        assertThat(SceneRules.status(List.of(CommandStatus.APPLIED, CommandStatus.BLOCKED))).isEqualTo(SceneRules.RunStatus.PARTIAL);
        assertThat(SceneRules.status(List.of(CommandStatus.FAILED, CommandStatus.BLOCKED))).isEqualTo(SceneRules.RunStatus.FAILED);
        assertThat(SceneRules.status(List.of(CommandStatus.APPLIED, CommandStatus.SENT))).isEqualTo(SceneRules.RunStatus.RUNNING);
        assertThat(SceneRules.status(List.of())).isEqualTo(SceneRules.RunStatus.FAILED);
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-128] Class A 예상 전달 시각 = 마지막 업링크 + 보고 주기(지났으면 다음 주기)")
    void classAExpected() {
        Instant now = Instant.parse("2026-03-02T00:00:00Z");
        assertThat(ClassADownlink.expectedDelivery(now.minus(Duration.ofMinutes(4)), 600, now)).isEqualTo(now.plus(Duration.ofMinutes(6)));
        assertThat(ClassADownlink.expectedDelivery(now.minus(Duration.ofMinutes(25)), 600, now)).isEqualTo(now.plus(Duration.ofMinutes(5)));
        assertThat(ClassADownlink.expectedDelivery(null, null, now)).isEqualTo(now.plus(Duration.ofMinutes(10)));
    }
}
