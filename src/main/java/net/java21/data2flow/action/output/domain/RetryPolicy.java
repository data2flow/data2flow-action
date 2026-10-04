package net.java21.data2flow.action.output.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * 출력 발송 재시도(BR-DSC-19): 1초부터 2배씩 최대 {@code max} 간격으로, 재시도 시작부터 {@code maxAge}(24시간)까지. 그 뒤에는 FAILED.
 */
public record RetryPolicy(Duration initial, Duration max, Duration maxAge) {

    /** {@code attempts}번 실패한 뒤 다음 시도까지 간격 */
    public Duration delayAfter(int attempts) {
        long ms = initial.toMillis();
        for (int i = 1; i < attempts && ms < max.toMillis(); i++) {
            ms *= 2;
        }
        return Duration.ofMillis(Math.min(ms, max.toMillis()));
    }

    /** 기한이 지났는가(재시도 시작 + maxAge 이후) */
    public boolean expired(Instant retryStartedAt, Instant now) {
        return !now.isBefore(retryStartedAt.plus(maxAge));
    }
}
