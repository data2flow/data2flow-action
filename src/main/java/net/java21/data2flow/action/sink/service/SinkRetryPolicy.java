package net.java21.data2flow.action.sink.service;

import net.java21.data2flow.action.sink.SinkProperties;
import net.java21.data2flow.action.sink.connector.SinkWriteException;

import java.time.Duration;
import java.time.Instant;

/**
 * Sink 재시도 규칙(BR-FLW-28): 일시 장애는 지수 백오프(1초부터 2배, 최대 60초)로 계속 다시 시도하되, 받은 지
 * {@code maxRetryAge}(기본 6시간)가 지나면 dead-letter로 보낸다. 영구 실패(인증·대상 없음·형식)는 바로 dead-letter. dead-letter는 24시간
 * 보관한 뒤 지운다.
 */
public record SinkRetryPolicy(Duration initial, Duration max, Duration maxAge, Duration deadRetention) {

    public static SinkRetryPolicy of(SinkProperties p) {
        return new SinkRetryPolicy(p.retryInitial(), p.retryMax(), p.maxRetryAge(), p.deadRetention());
    }

    /** {@code attempts}번 실패한 뒤 기다릴 시간 */
    public Duration backoff(int attempts) {
        long ms = initial.toMillis();
        for (int i = 1; i < attempts && ms < max.toMillis(); i++) {
            ms *= 2;
        }
        return Duration.ofMillis(Math.min(ms, max.toMillis()));
    }

    /** 이 실패 뒤 다시 시도할지(아니면 dead-letter) */
    public boolean retry(SinkWriteException failure, Instant createdAt, Instant now) {
        return failure.transientFailure() && !now.isAfter(createdAt.plus(maxAge));
    }

    /** dead-letter 보관 끝 */
    public Instant deadUntil(Instant deadAt) {
        return deadAt.plus(deadRetention);
    }
}
