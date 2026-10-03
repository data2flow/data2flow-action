package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.command.CommandPriority;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 충돌·폭주 방지(ACT-02.05, BR-ACT-07·08): 최소 간격, 진동(반대 명령 반복), 수동 우선.
 */
public final class RateGuards {

    private RateGuards() {
    }

    /**
     * 최소 간격(BR-ACT-07): 같은 기기·기능의 자동 명령(SCHEDULE·AUTO·AI)은 직전 명령 뒤 {@code minIntervalSec} 안에 다시 보낼 수 없다.
     * MANUAL·SAFETY는 예외다.
     *
     * @return 막히면 남은 시간(Retry-After), 통과면 빈 값
     */
    public static Optional<Duration> minInterval(CommandPriority priority, Instant lastRequestedAt, int minIntervalSec, Instant now) {
        if (!priority.automatic() || lastRequestedAt == null || minIntervalSec <= 0) {
            return Optional.empty();
        }
        Instant allowed = lastRequestedAt.plusSeconds(minIntervalSec);
        if (now.isBefore(allowed)) {
            return Optional.of(Duration.between(now, allowed));
        }
        return Optional.empty();
    }

    /**
     * 진동(BR-ACT-07): 창 안의 전원 목표(오래된 것부터)에 이번 목표를 붙였을 때 끝에서부터 서로 반대인 명령이 {@code flips}개 이상
     * 이어지면 진동이다. 예: 60초 안 on→off→on의 세 번째. 같은 방향 반복은 진동이 아니다. SAFETY는 보지 않는다.
     */
    public static boolean oscillating(CommandPriority priority, List<Boolean> recentTargets, boolean target, int flips) {
        if (priority == CommandPriority.SAFETY) {
            return false;
        }
        List<Boolean> seq = new ArrayList<>(recentTargets);
        seq.add(target);
        int run = 1;
        for (int i = seq.size() - 1; i > 0; i--) {
            if (seq.get(i).equals(seq.get(i - 1))) {
                break;
            }
            run++;
        }
        return run >= flips;
    }

    /**
     * 수동 우선(BR-ACT-08): 수동 우선이 걸린 동안 AUTO·AI 명령은 SKIPPED(MANUAL_OVERRIDE). SCHEDULE은 조직 설정(기본 막음).
     * MANUAL·SAFETY는 막지 않는다.
     */
    public static boolean manualOverrideBlocks(CommandPriority priority, Instant until, Instant now, boolean scheduleRespects) {
        if (until == null || !now.isBefore(until)) {
            return false;
        }
        return switch (priority) {
            case AUTO, AI, UNKNOWN -> true;
            case SCHEDULE -> scheduleRespects;
            case MANUAL, SAFETY -> false;
        };
    }
}
