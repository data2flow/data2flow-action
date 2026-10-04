package net.java21.data2flow.action.notification.domain;

import java.time.DayOfWeek;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Set;

/**
 * 요일·시각 구간(무음 반복, 당직 교대, 방해 금지). {@code from}이 {@code to}보다 늦으면 자정을 넘는 구간이고(예: 22:00~07:00),
 * 끝은 배타다(BR-RUL-14: 11:00:00은 구간 밖). {@code to}가 "24:00"이거나 {@code from}과 같으면 하루 전체다.
 *
 * @param days 시작 요일(비면 매일). 자정을 넘는 구간의 다음 날 부분은 시작 요일 다음 날이다
 * @param from 시작(포함)
 * @param to   끝(배타). null이면 하루 끝
 */
public record DailyWindow(Set<DayOfWeek> days, LocalTime from, LocalTime to) {

    public DailyWindow {
        days = days == null ? Set.of() : Set.copyOf(days);
        from = from == null ? LocalTime.MIDNIGHT : from;
    }

    /** "HH:mm" 읽기. "24:00"은 null(하루 끝) */
    public static LocalTime parse(String hhmm) {
        if (hhmm == null || hhmm.isBlank() || "24:00".equals(hhmm.trim())) {
            return null;
        }
        return LocalTime.parse(hhmm.trim());
    }

    /** 이 지역 시각이 구간 안인가 */
    public boolean contains(ZonedDateTime local) {
        LocalTime t = local.toLocalTime();
        DayOfWeek day = local.getDayOfWeek();
        boolean wholeDay = to == null || to.equals(from);
        if (wholeDay) {
            return dayMatches(day) && (to == null ? !t.isBefore(from) : true);
        }
        if (from.isBefore(to)) {
            return dayMatches(day) && !t.isBefore(from) && t.isBefore(to);
        }
        // 자정을 넘는 구간: 시작 요일의 from 이후, 또는 다음 날 to 이전
        return (dayMatches(day) && !t.isBefore(from)) || (dayMatches(day.minus(1)) && t.isBefore(to));
    }

    /** 구간 안이면 그 구간이 끝나는 지역 시각(배타 끝) */
    public ZonedDateTime endAfter(ZonedDateTime local) {
        LocalTime end = to == null ? LocalTime.MIDNIGHT : to;
        ZonedDateTime candidate = local.with(end);
        if (!candidate.isAfter(local)) {
            candidate = candidate.plusDays(1);
        }
        return candidate;
    }

    private boolean dayMatches(DayOfWeek day) {
        return days.isEmpty() || days.contains(day);
    }
}
