package net.java21.data2flow.action.output.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 재시도·접속 금지·템플릿 규칙(BR-DSC-19, CLAUDE.md §5) */
class OutputPoliciesTest {

    @Test
    @DisplayName("[DSC-04.01][BR-DSC-19][TC-DSC-126] 재시도 간격은 1초부터 2배씩 최대 5분, 재시도 시작 24시간이 지나면 기한 초과")
    void retry() {
        RetryPolicy p = new RetryPolicy(Duration.ofSeconds(1), Duration.ofMinutes(5), Duration.ofHours(24));
        assertThat(p.delayAfter(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(p.delayAfter(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(p.delayAfter(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(p.delayAfter(40)).isEqualTo(Duration.ofMinutes(5));
        Instant start = Instant.parse("2026-10-04T00:00:00Z");
        assertThat(p.expired(start, start.plus(Duration.ofHours(23)))).isFalse();
        assertThat(p.expired(start, start.plus(Duration.ofHours(24)))).isTrue();
    }

    @Test
    @DisplayName("[DSC-04.01][CLAUDE.md §5] 공용 브로커 iot-data.java21.net(대소문자·하위 이름·끝 점 포함)과 설정 금지 주소, 잘못된 주소는 접속 금지")
    void hostGuard() {
        assertThat(HostGuard.denied("wss://iot-data.java21.net:443/mqtt", List.of())).isTrue();
        assertThat(HostGuard.denied("mqtt://IOT-DATA.java21.net.", List.of())).isTrue();
        assertThat(HostGuard.denied("mqtts://edge.iot-data.java21.net:8883", List.of())).isTrue();
        assertThat(HostGuard.denied("https://hooks.example.org/in", List.of("example.org"))).isTrue();
        assertThat(HostGuard.denied("not a url", List.of())).isTrue();
        assertThat(HostGuard.denied("mqtt:///nohost", List.of())).isTrue();
        assertThat(HostGuard.denied("mqtt://127.0.0.1:1883", List.of())).isFalse();
        assertThat(HostGuard.deniedHost("[::1]", java.util.Arrays.asList(" ", null))).isFalse();
    }

    @Test
    @DisplayName("[DSC-04.01] 로직 없는 템플릿: {{x}}·{{{x}}}만 바꾸고 특수 문자는 그대로, 값이 없으면 빈 값")
    void templateLite() {
        assertThat(TemplateLite.render("a={{ a }} b={{{b}}} c={{c}} $1", Map.of("a", "$x\\y", "b", "2"))).isEqualTo("a=$x\\y b=2 c= $1");
        assertThat(TemplateLite.render("{{#section}}x{{/section}}", Map.of())).isEqualTo("{{#section}}x{{/section}}");
    }

    @Test
    @DisplayName("[DSC-04.01] 발송 결과 모양")
    void results() {
        assertThat(DeliveryResult.failure(net.java21.data2flow.contracts.output.OutputFailureKind.TLS, null, null, null).error()).isEqualTo("TLS");
        assertThat(DeliveryResult.success(204, null).ok()).isTrue();
    }
}
