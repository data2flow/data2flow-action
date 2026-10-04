package net.java21.data2flow.action.notification.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;

/** 무음 판정(BR-RUL-14, TC-RUL-062·063). 한 번 무음은 끝 배타, 반복 무음은 그 시간대의 요일·시각, 공간 무음은 하위 공간 포함 */
public final class SilenceMatcher {

    private SilenceMatcher() {
    }

    public static Optional<Silence> match(Collection<Silence> silences, AlarmInfo alarm, Instant at) {
        if (alarm == null) {
            return Optional.empty();
        }
        return silences.stream().filter(s -> active(s, at) && targets(s, alarm)).findFirst();
    }

    static boolean active(Silence s, Instant at) {
        if (s.startsAt() != null && at.isBefore(s.startsAt())) {
            return false;
        }
        if (s.endsAt() != null && !at.isBefore(s.endsAt())) {
            return false;
        }
        return s.recurring() == null || s.recurring().contains(at.atZone(s.zone()));
    }

    static boolean targets(Silence s, AlarmInfo a) {
        return switch (s.targetType()) {
            case "ALARM" -> a.id() == s.targetId();
            case "RULE" -> a.ruleId() != null && a.ruleId() == s.targetId();
            case "DEVICE" -> a.deviceId() != null && a.deviceId() == s.targetId();
            case "SPACE" -> a.spacePathIds().contains(s.targetId());
            default -> false;
        };
    }
}
