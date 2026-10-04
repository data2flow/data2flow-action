package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.action.common.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OnCallResolverTest {

    /** 평일 18:00~09:00 당직 A(5), 주말 종일 C(9), 오늘(월 2026-03-02 서울) 대체자 B(6) 20:00~익일 08:00 */
    private static OnCallSchedule schedule(boolean withOverride) {
        List<Map<String, Object>> shifts = new ArrayList<>();
        for (int d = 1; d <= 5; d++) {
            shifts.add(Map.of("dayOfWeek", d, "from", "18:00", "to", "09:00", "userId", "5"));
        }
        shifts.add(Map.of("dayOfWeek", 6, "from", "00:00", "to", "24:00", "userId", "9"));
        shifts.add(Map.of("dayOfWeek", 7, "from", "00:00", "to", "24:00", "userId", "9"));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("timezone", "Asia/Seoul");
        m.put("shifts", shifts);
        m.put("overrides", withOverride ? List.of(Map.of("startsAt", "2026-03-02T11:00:00Z", "endsAt", "2026-03-02T23:00:00Z",
                "originalUserId", "5", "substituteUserId", "6")) : List.of());
        return OnCallSchedule.from(Json.MAPPER.valueToTree(m));
    }

    @ParameterizedTest(name = "[RUL-05.03][BR-RUL-19][TC-RUL-100] {0} → 당직 {1}")
    @CsvSource({
            "2026-03-02T10:00:00Z, 5",      // 월 19:00 → 주간 교대 A
            "2026-03-02T12:00:00Z, 6",      // 월 21:00 → 대체 근무 B가 우선
            "2026-03-02T23:30:00Z, 5",      // 화 08:30 → 대체 끝, 월 밤 교대(자정 넘음) A
            "2026-03-03T01:00:00Z, -1",     // 화 10:00 → 비어 있음
            "2026-03-07T03:00:00Z, 9"       // 토 12:00 → 주말 C
    })
    void resolve(String at, long expected) {
        assertThat(schedule(true).onCallAt(Instant.parse(at)).orElse(-1L)).isEqualTo(expected);
    }

    @Test
    @DisplayName("[RUL-05.03][BR-RUL-19][TC-RUL-100] 대체 근무가 없으면 주간 교대, 당직표가 비면 빈 값(정책의 다른 수신자·ADMIN으로 대신)")
    void emptySchedule() {
        assertThat(schedule(false).onCallAt(Instant.parse("2026-03-02T12:00:00Z"))).contains(5L);
        assertThat(OnCallSchedule.EMPTY.onCallAt(Instant.parse("2026-03-02T12:00:00Z"))).isEmpty();
    }
}
