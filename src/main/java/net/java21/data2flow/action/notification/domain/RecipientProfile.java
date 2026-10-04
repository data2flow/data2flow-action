package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 수신 사용자(core API-RUL-42): 역할, 언어·시간대, 최소 심각도(OPS-06.05), 방해 금지(RUL-05.04), 메신저 연결, 공간 범위(BR-RUL-12).
 */
public record RecipientProfile(long userId, String role, boolean active, String locale, ZoneId zone, AlarmSeverity minSeverity,
                               DailyWindow dnd, boolean dndAllowCritical, Map<String, String> links, boolean unrestricted,
                               Set<Long> allowedSpaceIds) {

    public RecipientProfile {
        links = links == null ? Map.of() : Map.copyOf(links);
        allowedSpaceIds = allowedSpaceIds == null ? Set.of() : Set.copyOf(allowedSpaceIds);
        zone = zone == null ? ZoneId.of("Asia/Seoul") : zone;
    }

    public static RecipientProfile from(JsonNode r) {
        Map<String, String> links = new LinkedHashMap<>();
        r.path("links").properties().forEach(e -> links.put(e.getKey(), e.getValue().asString()));
        Set<Long> allowed = new LinkedHashSet<>();
        r.path("spaceScope").path("allowedSpaceIds").forEach(n -> allowed.add(n.asLong()));
        JsonNode dnd = r.path("dnd");
        DailyWindow window = dnd.path("from").isString() && dnd.path("to").isString()
                ? new DailyWindow(Set.of(), DailyWindow.parse(dnd.path("from").asString()), DailyWindow.parse(dnd.path("to").asString()))
                : null;
        AlarmSeverity min = null;
        if (r.path("minSeverity").isString()) {
            try {
                min = AlarmSeverity.valueOf(r.path("minSeverity").asString());
            } catch (IllegalArgumentException ignored) {
                min = null;
            }
        }
        ZoneId zone;
        try {
            zone = r.path("timezone").isString() ? ZoneId.of(r.path("timezone").asString()) : null;
        } catch (RuntimeException e) {
            zone = null;
        }
        return new RecipientProfile(r.path("userId").asLong(), r.path("role").asString(""), r.path("active").asBoolean(true),
                r.path("locale").isString() ? r.path("locale").asString() : null, zone, min, window,
                dnd.path("allowCritical").asBoolean(false), links, r.path("spaceScope").path("unrestricted").asBoolean(true), allowed);
    }

    /** 알람 공간을 볼 수 있는가(BR-RUL-12). 공간이 없는 알람이면 true */
    public boolean canSee(java.util.List<Long> spacePathIds) {
        if (unrestricted || spacePathIds == null || spacePathIds.isEmpty()) {
            return true;
        }
        return spacePathIds.stream().anyMatch(allowedSpaceIds::contains);
    }

    /** 최소 심각도를 넘는가. 설정이 없거나 심각도를 모르면 true */
    public boolean wants(AlarmSeverity severity) {
        return minSeverity == null || severity == null || severity == AlarmSeverity.UNKNOWN || severity.atLeast(minSeverity);
    }

    /**
     * 방해 금지 중이면 끝나는 시각(OPS-06.05: 예외가 없으면 끝나는 시각에 요약 1건). CRITICAL 예외가 켜져 있고 CRITICAL이면 빈 값
     */
    public Optional<Instant> dndUntil(Instant now, AlarmSeverity severity) {
        if (dnd == null) {
            return Optional.empty();
        }
        if (dndAllowCritical && severity == AlarmSeverity.CRITICAL) {
            return Optional.empty();
        }
        ZonedDateTime local = now.atZone(zone);
        return dnd.contains(local) ? Optional.of(dnd.endAfter(local).toInstant()) : Optional.empty();
    }
}
