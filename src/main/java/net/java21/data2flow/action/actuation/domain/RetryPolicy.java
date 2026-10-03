package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Duration;

/**
 * 드라이버 호출 재시도(BR-ACT-14, drivers.retry {@code {maxAttempts:3, initialMs:1000, multiplier:2, maxMs:10000}}).
 *
 * @param maxAttempts 최대 호출 횟수(첫 호출 포함)
 * @param initialMs   첫 재시도 간격
 * @param multiplier  간격 배수
 * @param maxMs       최대 간격
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RetryPolicy(int maxAttempts, long initialMs, double multiplier, long maxMs) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(3, 1000, 2.0, 10_000);

    public RetryPolicy {
        maxAttempts = maxAttempts < 1 ? 3 : maxAttempts;
        initialMs = initialMs <= 0 ? 1000 : initialMs;
        multiplier = multiplier < 1 ? 2.0 : multiplier;
        maxMs = maxMs <= 0 ? 10_000 : maxMs;
    }

    /** {@code attempt}번째 호출이 실패한 뒤 다음 호출까지 기다릴 시간(지수 백오프). attempt는 1부터 */
    public Duration backoff(int attempt) {
        double ms = initialMs * Math.pow(multiplier, Math.max(0, attempt - 1));
        return Duration.ofMillis((long) Math.min(ms, maxMs));
    }

    /** 다시 호출할 수 있는지(지금까지 {@code attempts}번 불렀음) */
    public boolean canRetry(int attempts) {
        return attempts < maxAttempts;
    }
}
