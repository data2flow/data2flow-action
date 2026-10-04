package net.java21.data2flow.action.notification.domain;

import tools.jackson.databind.JsonNode;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 당직표(core API-RUL-44, RUL-05.03). 당직자 계산: 대리 근무 &gt; 주간 교대(BR-RUL-19). 교대는 그 시간대 기준이고 자정을 넘을 수 있다
 * (예: 평일 18:00~09:00 → 월 18시부터 화 9시 전까지).
 */
public record OnCallSchedule(ZoneId zone, List<Shift> shifts, List<Override> overrides) {

    public OnCallSchedule {
        zone = zone == null ? ZoneId.of("Asia/Seoul") : zone;
        shifts = shifts == null ? List.of() : List.copyOf(shifts);
        overrides = overrides == null ? List.of() : List.copyOf(overrides);
    }

    public static final OnCallSchedule EMPTY = new OnCallSchedule(null, List.of(), List.of());

    public record Shift(DayOfWeek day, DailyWindow window, long userId) {
    }

    public record Override(Instant startsAt, Instant endsAt, long substituteUserId) {
    }

    /** 이 시각의 당직자. 비어 있으면 빈 값(BR-RUL-19: 그때는 정책의 다른 수신자, 없으면 조직 ADMIN) */
    public Optional<Long> onCallAt(Instant at) {
        for (Override o : overrides) {
            if (!at.isBefore(o.startsAt()) && at.isBefore(o.endsAt())) {
                return Optional.of(o.substituteUserId());
            }
        }
        for (Shift s : shifts) {
            if (s.window().contains(at.atZone(zone))) {
                return Optional.of(s.userId());
            }
        }
        return Optional.empty();
    }

    public static OnCallSchedule from(JsonNode r) {
        List<Shift> shifts = new ArrayList<>();
        r.path("shifts").forEach(s -> {
            DayOfWeek day = DayOfWeek.of(s.path("dayOfWeek").asInt());
            shifts.add(new Shift(day, new DailyWindow(Set.of(day), DailyWindow.parse(s.path("from").asString("00:00")),
                    DailyWindow.parse(s.path("to").asString("24:00"))), s.path("userId").asLong()));
        });
        List<Override> overrides = new ArrayList<>();
        r.path("overrides").forEach(o -> overrides.add(new Override(Instant.parse(o.path("startsAt").asString()),
                Instant.parse(o.path("endsAt").asString()), o.path("substituteUserId").asLong())));
        ZoneId zone = r.path("timezone").isString() ? ZoneId.of(r.path("timezone").asString()) : null;
        return new OnCallSchedule(zone, shifts, overrides);
    }
}
