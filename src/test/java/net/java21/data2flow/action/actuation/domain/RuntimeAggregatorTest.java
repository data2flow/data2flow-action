package net.java21.data2flow.action.actuation.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** 가동 집계(ACT-08.02, BR-ACT-21) */
class RuntimeAggregatorTest {

    private static final Instant DAY = Instant.parse("2026-03-01T15:00:00Z");   // 2026-03-02 00:00 Asia/Seoul
    private static final Instant END = DAY.plusSeconds(86_400);

    @Test
    @DisplayName("[ACT-08.02][AT-ACT-12.2][TC-ACT-135] 가상 에어컨 하루 6시간 가동, 정격 1.2kW → 가동 6h, 추정 7.2kWh(RATED)")
    void sixHours() {
        var d = RuntimeAggregator.aggregate(false, List.of(
                new RuntimeAggregator.PowerChange(DAY.plusSeconds(9 * 3600), true),
                new RuntimeAggregator.PowerChange(DAY.plusSeconds(12 * 3600), false),
                new RuntimeAggregator.PowerChange(DAY.plusSeconds(13 * 3600), true),
                new RuntimeAggregator.PowerChange(DAY.plusSeconds(16 * 3600), false)), DAY, END, 1200.0, null);
        assertThat(d.onSeconds()).isEqualTo(6 * 3600);
        assertThat(d.cycles()).isEqualTo(2);
        assertThat(d.energyWh()).isEqualTo(7200.0);
        assertThat(d.energySource()).isEqualTo("RATED");
    }

    @Test
    @DisplayName("[ACT-08.02][TC-ACT-136] BR-ACT-21 규칙 표: 전날부터 켜짐·보고 전력 우선·정격 없음·범위 밖 변화 무시")
    void rules() {
        // 전날부터 켜져 있다가 2시간 뒤 꺼짐
        var carried = RuntimeAggregator.aggregate(true, List.of(new RuntimeAggregator.PowerChange(DAY.plusSeconds(7200), false)), DAY, END,
                1000.0, null);
        assertThat(carried.onSeconds()).isEqualTo(7200);
        assertThat(carried.cycles()).isZero();
        // 전력 보고가 있으면 보고값
        var reported = RuntimeAggregator.aggregate(true, List.of(), DAY, END, 1000.0, 5000.0);
        assertThat(reported.energyWh()).isEqualTo(5000.0);
        assertThat(reported.energySource()).isEqualTo("REPORTED");
        assertThat(reported.onSeconds()).isEqualTo(86_400);
        // 정격 전력이 없으면 에너지 없음
        var unknown = RuntimeAggregator.aggregate(false, List.of(new RuntimeAggregator.PowerChange(DAY.plusSeconds(60), true)), DAY,
                DAY.plusSeconds(120), null, null);
        assertThat(unknown.onSeconds()).isEqualTo(60);
        assertThat(unknown.energyWh()).isNull();
        assertThat(unknown.energySource()).isNull();
        // 하루 밖 변화는 무시
        var outside = RuntimeAggregator.aggregate(false, List.of(new RuntimeAggregator.PowerChange(END.plusSeconds(10), true)), DAY, END,
                1000.0, null);
        assertThat(outside.onSeconds()).isZero();
    }
}
