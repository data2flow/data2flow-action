package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.capability.DeviceShadow;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.SourceType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 출처·우선순위·목표 상태·보고 순서·재시도 규칙. TC-ACT-040·046·047 */
class PolicyTest {

    @Test
    @DisplayName("[ACT-02.03][BR-ACT-15·24][TC-ACT-040] 우선순위는 출처가 정하고, AI 출처는 승인자가 있어야 한다")
    void sourcePolicy() {
        assertThat(CommandSource.user(7).priority()).isEqualTo(CommandPriority.MANUAL);
        assertThat(CommandSource.flow("f", 1, "n", "m").priority()).isEqualTo(CommandPriority.AUTO);
        assertThat(CommandSource.schedule(3).priority()).isEqualTo(CommandPriority.SCHEDULE);
        assertThat(CommandSource.system().priority()).isEqualTo(CommandPriority.SAFETY);
        assertThat(CommandSource.ai("s-1", 7).requireComplete().priority()).isEqualTo(CommandPriority.AI);
        assertThatThrownBy(() -> new CommandSource(SourceType.AI, null, null, null, null, null, null, null, null, "s-1", null).requireComplete())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("[ACT-02.04][BR-ACT-03][TC-ACT-046] 명령은 목표 상태 설정: 같은 명령을 여러 번 적용해도 결과가 같다")
    void desiredStateIdempotent() {
        DeviceShadow once = DeviceShadow.EMPTY.withDesired("Switch", Map.of("on", true));
        DeviceShadow twice = once.withDesired("Switch", Map.of("on", true));
        assertThat(twice.desired()).isEqualTo(once.desired());
        assertThat(PowerState.target("Switch", Map.of("on", true))).contains(true);
        assertThat(PowerState.target("Thermostat", Map.of("mode", "off"))).contains(false);
        assertThat(PowerState.target("Thermostat", Map.of("targetTemperature", 24))).isEmpty();
        assertThat(PowerState.target("Ventilation", Map.of("mode", "auto"))).contains(true);
        assertThat(PowerState.target("Dimmer", Map.of("level", 3))).isEmpty();
        assertThat(PowerState.target("Switch", null)).isEmpty();
    }

    @Test
    @DisplayName("[ACT-02.04][BR-ACT-05][TC-ACT-047] 보고는 버전이 클 때만 반영, 오래된 보고는 버린다")
    void reportOrdering() {
        DeviceShadow v8 = DeviceShadow.EMPTY.withReported(8, Map.of("Switch", Map.of("on", true)), Instant.EPOCH).orElseThrow();
        assertThat(v8.withReported(7, Map.of("Switch", Map.of("on", false)), Instant.EPOCH)).isEmpty();
        assertThat(v8.withReported(8, Map.of("Switch", Map.of("on", false)), Instant.EPOCH)).isEmpty();
        assertThat(v8.withReported(9, Map.of("Switch", Map.of("on", false)), Instant.EPOCH)).isPresent();
    }

    @Test
    @DisplayName("[ACT-07.03][BR-ACT-14] 재시도: 지수 백오프(1·2·4초, 최대 10초), 최대 3회")
    void retryPolicy() {
        RetryPolicy p = RetryPolicy.DEFAULT;
        assertThat(p.backoff(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(p.backoff(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(p.backoff(5)).isEqualTo(Duration.ofSeconds(10));
        assertThat(p.canRetry(2)).isTrue();
        assertThat(p.canRetry(3)).isFalse();
        assertThat(new RetryPolicy(0, 0, 0, 0)).isEqualTo(RetryPolicy.DEFAULT);
    }

    @Test
    @DisplayName("[ACT-01.03] 프로필 기본값: 설정이 없으면 조직 기본(수동 우선 30분·최소 간격 10초·진동 60초 3번·유효 600초)")
    void defaults() {
        ControlSettings s = ControlSettings.DEFAULT;
        assertThat(s.manualOverrideMinutes()).isEqualTo(30);
        assertThat(s.minIntervalSec()).isEqualTo(10);
        assertThat(s.oscillation()).isEqualTo(new ControlSettings.Oscillation(60, 3));
        assertThat(s.defaultValiditySec()).isEqualTo(600);
        assertThat(s.scheduleRespectsManualOverride()).isTrue();
        assertThat(s.limitsFor("Switch")).isEmpty();
        ControlProfile p = new ControlProfile(1, 1, null, null, null, false, null, null, null, null, null);
        assertThat(p.controllable()).isFalse();
        DriverBinding b = new DriverBinding(1L, "VIRTUAL", null, null, 0, null);
        assertThat(b.ackTimeout(Duration.ofSeconds(30))).isEqualTo(Duration.ofSeconds(30));
        assertThat(b.applyTimeout(Duration.ofSeconds(60))).isEqualTo(Duration.ofSeconds(60));
    }
}
