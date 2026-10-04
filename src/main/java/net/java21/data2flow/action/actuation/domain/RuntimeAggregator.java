package net.java21.data2flow.action.actuation.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * 장비 가동 집계(BR-ACT-21, ACT-08.02). 보고된 전원 상태 구간(켜짐·꺼짐)으로 하루 가동 시간과 켜짐 횟수를 계산하고, 전력 보고가 있으면 그 값,
 * 없으면 모델 정격 전력 × 가동 시간으로 에너지를 추정한다.
 */
public final class RuntimeAggregator {

    /** 전원 상태가 바뀐 시점 */
    public record PowerChange(Instant at, boolean on) {
    }

    /**
     * 하루 집계.
     *
     * @param onSeconds    가동 시간(초)
     * @param cycles       꺼짐 → 켜짐 횟수
     * @param energyWh     에너지(Wh). 정격·보고가 모두 없으면 null
     * @param energySource RATED·REPORTED·null
     */
    public record Daily(long onSeconds, int cycles, Double energyWh, String energySource) {
    }

    private RuntimeAggregator() {
    }

    /**
     * @param initialOn       하루 시작 때 켜져 있었는가
     * @param changes         하루 안의 변화(시각 순)
     * @param dayStart        하루 시작
     * @param dayEnd          하루 끝(오늘이면 지금)
     * @param ratedPowerW     정격 전력. 없으면 null
     * @param reportedEnergyWh 보고된 에너지. 있으면 이 값을 쓴다
     */
    public static Daily aggregate(boolean initialOn, List<PowerChange> changes, Instant dayStart, Instant dayEnd, Double ratedPowerW,
                                  Double reportedEnergyWh) {
        boolean on = initialOn;
        Instant since = dayStart;
        long seconds = 0;
        int cycles = 0;
        for (PowerChange c : changes) {
            if (c.at().isBefore(dayStart) || !c.at().isBefore(dayEnd)) {
                continue;
            }
            if (on) {
                seconds += Duration.between(since, c.at()).getSeconds();
            }
            if (!on && c.on()) {
                cycles++;
            }
            on = c.on();
            since = c.at();
        }
        if (on && since.isBefore(dayEnd)) {
            seconds += Duration.between(since, dayEnd).getSeconds();
        }
        if (reportedEnergyWh != null) {
            return new Daily(seconds, cycles, reportedEnergyWh, "REPORTED");
        }
        if (ratedPowerW != null && ratedPowerW > 0) {
            return new Daily(seconds, cycles, ratedPowerW * seconds / 3600.0, "RATED");
        }
        return new Daily(seconds, cycles, null, null);
    }
}
