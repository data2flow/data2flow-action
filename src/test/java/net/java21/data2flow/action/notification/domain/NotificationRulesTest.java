package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationProperties;
import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import net.java21.data2flow.contracts.notification.NotificationEvents;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** 재알림(BR-RUL-13)·재시도(BR-RUL-17)·템플릿(RUL-03.04)·수신 설정(OPS-06.05) 규칙 표 */
class NotificationRulesTest {

    private static final Instant T0 = Instant.parse("2026-03-02T00:00:00Z");

    @ParameterizedTest(name = "[RUL-03.02][BR-RUL-13][TC-RUL-070] {0}, 마지막 발송 {1}분 전, 간격 30분 → 건너뜀 {2}")
    @CsvSource({"alarm.reraised,10,true", "alarm.reraised,29,true", "alarm.reraised,30,false", "alarm.reraised,45,false",
            "alarm.raised,1,false", "alarm.cleared,1,false", "alarm.reraised,-1,false"})
    void renotify(String event, int minutesAgo, boolean skip) {
        Instant last = minutesAgo < 0 ? null : T0.minus(Duration.ofMinutes(minutesAgo));
        assertThat(RenotifyPolicy.skip(event, last, T0, Duration.ofMinutes(30)).isPresent()).isEqualTo(skip);
    }

    @Test
    @DisplayName("[RUL-03.02][BR-RUL-13][TC-RUL-070] 해제 알림은 정책의 notifyOnClear일 때만(정책이 없으면 요청대로)")
    void clearNotice() {
        PolicyDefinition off = policy(false, List.of());
        PolicyDefinition on = policy(true, List.of());
        assertThat(RenotifyPolicy.sendClear(NotificationEvents.ALARM_CLEARED, off)).isFalse();
        assertThat(RenotifyPolicy.sendClear(NotificationEvents.ALARM_CLEARED, on)).isTrue();
        assertThat(RenotifyPolicy.sendClear(NotificationEvents.ALARM_CLEARED, null)).isTrue();
        assertThat(RenotifyPolicy.sendClear(NotificationEvents.ALARM_RAISED, off)).isTrue();
    }

    @Test
    @DisplayName("[RUL-03.05][BR-RUL-17][TC-RUL-079] 일시 실패는 30초·2분·10분·30분·1시간으로 최대 5회, Retry-After가 있으면 따르고, 영구 실패·소진은 끝")
    void retry() {
        NotificationRetryPolicy p = new NotificationRetryPolicy(NotificationProperties.SPEC_BACKOFFS);
        assertThat(p.maxRetries()).isEqualTo(5);
        assertThat(p.next(1, true, null)).contains(Duration.ofSeconds(30));
        assertThat(p.next(2, true, null)).contains(Duration.ofMinutes(2));
        assertThat(p.next(3, true, null)).contains(Duration.ofMinutes(10));
        assertThat(p.next(4, true, null)).contains(Duration.ofMinutes(30));
        assertThat(p.next(5, true, null)).contains(Duration.ofHours(1));
        assertThat(p.next(6, true, null)).isEmpty();
        assertThat(p.next(1, false, null)).isEmpty();
        assertThat(p.next(1, true, Duration.ofSeconds(7))).contains(Duration.ofSeconds(7));
        assertThat(p.next(0, true, null)).isEmpty();
    }

    @Test
    @DisplayName("[RUL-03.04][TC-RUL-078] 템플릿 {{device.name}}·{{value}}는 변수(중첩·점 키)로 치환, 없는 변수는 빈 값, 숫자는 불필요한 0 제거")
    void template() {
        Map<String, Object> vars = Map.of("device", Map.of("name", "실습실 온도계"), "value", 27.60, "count", 3.0, "space.name", "실습실");
        assertThat(TemplateRenderer.render("{{device.name}} {{value}}℃ x{{count}} @{{space.name}} {{missing}}", vars))
                .isEqualTo("실습실 온도계 27.6℃ x3 @실습실 ");
        assertThat(TemplateRenderer.render(null, vars)).isEmpty();
        assertThat(NotificationTexts.defaultTemplate("alarm.raised", "en")[1]).contains("Value");
        assertThat(NotificationTexts.text("digest", "zh-CN", 3, "x")).contains("3");
        assertThat(NotificationTexts.text("ack", "fr")).isEqualTo("Acknowledge");
    }

    @Test
    @DisplayName("[OPS-06.05][AT-OPS-13.1·13.2][TC-OPS-068·070] 최소 심각도 MAJOR면 MINOR 거름, 방해 금지 22~07시: CRITICAL 예외 켬 → 23시 CRITICAL 보냄, 그 밖은 07시까지 대기")
    void preferences() {
        RecipientProfile p = RecipientProfile.from(Json.MAPPER.valueToTree(Map.of("userId", "5", "role", "OPERATOR", "active", true,
                "timezone", "Asia/Seoul", "minSeverity", "MAJOR",
                "dnd", Map.of("from", "22:00", "to", "07:00", "allowCritical", true),
                "spaceScope", Map.of("unrestricted", false, "allowedSpaceIds", List.of(7)))));
        assertThat(p.wants(AlarmSeverity.MINOR)).isFalse();
        assertThat(p.wants(AlarmSeverity.MAJOR)).isTrue();
        assertThat(p.wants(AlarmSeverity.CRITICAL)).isTrue();
        Instant elevenPm = Instant.parse("2026-03-02T14:00:00Z");   // 서울 23:00
        assertThat(p.dndUntil(elevenPm, AlarmSeverity.CRITICAL)).isEmpty();
        assertThat(p.dndUntil(elevenPm, AlarmSeverity.MAJOR)).contains(Instant.parse("2026-03-02T22:00:00Z"));   // 다음 날 07:00
        assertThat(p.dndUntil(Instant.parse("2026-03-02T03:00:00Z"), AlarmSeverity.MAJOR)).isEmpty();          // 낮 12:00
        assertThat(p.canSee(List.of(1L, 7L, 31L))).isTrue();
        assertThat(p.canSee(List.of(1L, 8L))).isFalse();
        assertThat(p.canSee(List.of())).isTrue();
    }

    @Test
    @DisplayName("[RUL-03.03][BR-RUL-16][TC-RUL-077] 에스컬레이션은 최대 3단계: 4단계 정책은 거부")
    void maxThreeSteps() {
        assertThatThrownBy(() -> policy(false, List.of(1, 2, 3, 4))).isInstanceOf(IllegalArgumentException.class);
        assertThat(policy(false, List.of(1, 2, 3)).step(3)).isPresent();
    }

    static PolicyDefinition policy(boolean notifyOnClear, List<Integer> steps) {
        return PolicyDefinition.from(Json.MAPPER.valueToTree(Map.of("notificationPolicyId", "3", "organizationId", "1", "minSeverity", "MINOR",
                "recipients", List.of(Map.of("type", "ON_CALL")), "channels", List.of("TELEGRAM"), "renotifyMinutes", 30,
                "aggregateWindowSec", 0, "notifyOnClear", notifyOnClear,
                "steps", steps.stream().map(s -> Map.of("stepNo", s, "waitMinutes", 10, "recipients", List.of(Map.of("type", "USER", "id", "6"))))
                        .toList())));
    }
}
