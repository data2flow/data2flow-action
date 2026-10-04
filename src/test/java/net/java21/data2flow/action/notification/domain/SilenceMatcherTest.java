package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SilenceMatcherTest {

    private static final AlarmInfo ALARM = AlarmInfo.from(Json.MAPPER.valueToTree(NotificationFixtures.alarm(9001, "MAJOR", "ACTIVE")));

    private static Silence silence(Map<String, Object> m) {
        return Silence.from(Json.MAPPER.valueToTree(m));
    }

    @ParameterizedTest(name = "[RUL-02.07][BR-RUL-14][TC-RUL-062] 대상 {0}:{1} → 무음 {2}")
    @CsvSource({"ALARM,9001,true", "ALARM,9002,false", "RULE,12,true", "RULE,13,false", "DEVICE,15,true", "DEVICE,16,false",
            "SPACE,31,true", "SPACE,7,true", "SPACE,1,true", "SPACE,99,false", "UNKNOWN,1,false"})
    void targets(String type, long id, boolean expected) {
        Silence s = silence(Map.of("silenceId", 1, "kind", "ONE_TIME", "target", Map.of("type", type, "id", id)));
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T01:00:00Z")).isPresent()).isEqualTo(expected);
    }

    @Test
    @DisplayName("[RUL-02.07][AT-RUL-09.2][TC-RUL-063] 무음 10:00~11:00(Asia/Seoul) → 10:59:59 무음, 11:00:00 발송(끝 배타). 무음이 없으면 발송")
    void oneTimeEndExclusive() {
        Silence s = silence(Map.of("silenceId", 1, "kind", "ONE_TIME", "target", Map.of("type", "RULE", "id", 12),
                "startsAt", "2026-03-02T01:00:00Z", "endsAt", "2026-03-02T02:00:00Z"));
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T00:59:59Z"))).isEmpty();
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T01:00:00Z"))).isPresent();
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T01:59:59Z"))).isPresent();
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T02:00:00Z"))).isEmpty();
        assertThat(SilenceMatcher.match(List.of(), ALARM, Instant.parse("2026-03-02T01:30:00Z"))).isEmpty();
        assertThat(SilenceMatcher.match(List.of(s), null, Instant.parse("2026-03-02T01:30:00Z"))).isEmpty();
    }

    @Test
    @DisplayName("[RUL-02.07][AT-RUL-09.3][TC-RUL-063] 반복 무음 매주 일요일(Asia/Seoul) → 일 23:59:59 무음, 월 00:00 발송, 토요일 발송")
    void recurringSunday() {
        Silence s = silence(Map.of("silenceId", 2, "kind", "RECURRING", "target", Map.of("type", "SPACE", "id", 31),
                "recurrence", Map.of("daysOfWeek", List.of(7), "from", "00:00", "to", "24:00", "timezone", "Asia/Seoul")));
        // 2026-03-08은 일요일. 서울 23:59:59 = UTC 14:59:59
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-08T14:59:59Z"))).isPresent();
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-07T15:00:00Z"))).isPresent();   // 일 00:00
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-08T15:00:00Z"))).isEmpty();     // 월 00:00
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-07T14:59:59Z"))).isEmpty();     // 토 23:59:59
    }

    @Test
    @DisplayName("[RUL-02.07][BR-RUL-14] 반복 무음이 자정을 넘으면(22:00~07:00) 다음 날 아침까지 무음")
    void overnightRecurring() {
        Silence s = silence(Map.of("silenceId", 3, "kind", "RECURRING", "target", Map.of("type", "DEVICE", "id", 15),
                "recurrence", Map.of("daysOfWeek", List.of(1, 2, 3, 4, 5), "from", "22:00", "to", "07:00", "timezone", "Asia/Seoul")));
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T13:30:00Z"))).isPresent();   // 월 22:30
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T21:59:00Z"))).isPresent();   // 화 06:59
        assertThat(SilenceMatcher.match(List.of(s), ALARM, Instant.parse("2026-03-02T22:00:00Z"))).isEmpty();     // 화 07:00
    }
}
