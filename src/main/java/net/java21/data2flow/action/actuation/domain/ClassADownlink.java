package net.java21.data2flow.action.actuation.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * LoRaWAN Class A 다운링크 예상 전달 시각(ACT-07.02): Class A 기기는 업링크 직후 수신 창에서만 다운링크를 받으므로, 다음 업링크 예상 시각
 * = 마지막 업링크 + 보고 주기(이미 지났으면 주기를 더 더한다)이다.
 */
public final class ClassADownlink {

    /** 보고 주기를 모를 때(LoRaWAN 센서 기본 10분) */
    public static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(10);

    private ClassADownlink() {
    }

    public static Instant expectedDelivery(Instant lastUplink, Integer reportIntervalSec, Instant now) {
        Duration interval = reportIntervalSec == null || reportIntervalSec <= 0 ? DEFAULT_INTERVAL : Duration.ofSeconds(reportIntervalSec);
        if (lastUplink == null) {
            return now.plus(interval);
        }
        Instant next = lastUplink.plus(interval);
        if (next.isAfter(now)) {
            return next;
        }
        long periods = Duration.between(lastUplink, now).toMillis() / interval.toMillis() + 1;
        return lastUplink.plus(interval.multipliedBy(periods));
    }
}
