package net.java21.data2flow.action.notification.domain;

import tools.jackson.databind.JsonNode;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 무음(core {@code silences}, API-RUL-43, RUL-02.07). 알람은 기록하고 알림만 SKIPPED(SILENCED)로 남긴다(BR-RUL-14).
 *
 * @param targetType RULE·DEVICE·SPACE(하위 포함)·ALARM
 * @param startsAt   시작(포함). null이면 처음부터
 * @param endsAt     끝(배타). null이면 끝 없음
 * @param recurring  반복 무음(요일·시각, 그 시간대 기준)
 */
public record Silence(long silenceId, String targetType, long targetId, Instant startsAt, Instant endsAt, DailyWindow recurring,
                      ZoneId zone) {

    public static Silence from(JsonNode r) {
        DailyWindow window = null;
        ZoneId zone = ZoneId.of("Asia/Seoul");
        JsonNode rec = r.path("recurrence");
        if ("RECURRING".equals(r.path("kind").asString("")) && rec.isObject()) {
            Set<DayOfWeek> days = new LinkedHashSet<>();
            rec.path("daysOfWeek").forEach(d -> days.add(DayOfWeek.of(d.asInt())));
            window = new DailyWindow(days, DailyWindow.parse(rec.path("from").asString("00:00")),
                    DailyWindow.parse(rec.path("to").asString("24:00")));
            if (rec.path("timezone").isString()) {
                zone = ZoneId.of(rec.path("timezone").asString());
            }
        }
        return new Silence(r.path("silenceId").asLong(r.path("id").asLong()), r.path("target").path("type").asString(""),
                r.path("target").path("id").asLong(), instant(r.path("startsAt")), instant(r.path("endsAt")), window, zone);
    }

    private static Instant instant(JsonNode n) {
        return n.isString() ? Instant.parse(n.asString()) : null;
    }
}
