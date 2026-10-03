package net.java21.data2flow.action.actuation.domain;

import java.time.Instant;
import java.time.LocalDate;

/**
 * 기기별 보호 판정 상태({@code data2flow_action.protection_state}).
 *
 * @param lastOnAt    마지막으로 켜진 시각(보고 기준)
 * @param lastOffAt   마지막으로 꺼진 시각
 * @param cyclesToday {@code cyclesDate}의 켜기 횟수
 * @param cyclesDate  날짜(UTC)
 */
public record ProtectionState(Instant lastOnAt, Instant lastOffAt, int cyclesToday, LocalDate cyclesDate) {

    public static final ProtectionState EMPTY = new ProtectionState(null, null, 0, null);

    /** 오늘 켜기 횟수(날짜가 바뀌었으면 0) */
    public int cyclesOn(LocalDate today) {
        return today.equals(cyclesDate) ? cyclesToday : 0;
    }

    /** 전원 변화 반영: 켜짐이면 마지막 켜짐 시각과 오늘 횟수, 꺼짐이면 마지막 꺼짐 시각 */
    public ProtectionState withPower(boolean on, Instant at, LocalDate today) {
        if (on) {
            return new ProtectionState(at, lastOffAt, cyclesOn(today) + 1, today);
        }
        return new ProtectionState(lastOnAt, at, cyclesOn(today), today);
    }
}
