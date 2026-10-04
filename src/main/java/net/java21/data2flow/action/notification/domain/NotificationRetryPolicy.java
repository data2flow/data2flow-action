package net.java21.data2flow.action.notification.domain;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * 발송 재시도(BR-RUL-17·BR-OPS-06·OPS-06.03, TC-RUL-079): 일시 실패만 지수 백오프(기본 30초·2분·10분·30분·1시간)로 최대 5회 다시 보내고,
 * 상대가 대기 시간(429 {@code Retry-After}, TC-RUL-080)을 알려 주면 그것을 따른다. 영구 실패는 바로 FAILED.
 */
public record NotificationRetryPolicy(List<Duration> backoffs) {

    public NotificationRetryPolicy {
        backoffs = List.copyOf(backoffs);
    }

    /** 최대 재시도 횟수 */
    public int maxRetries() {
        return backoffs.size();
    }

    /**
     * 다음 시도까지 기다릴 시간. 더 시도하지 않으면 빈 값.
     *
     * @param attempts   지금까지 채널을 부른 횟수(첫 시도 포함, 1부터)
     * @param retryable  일시 실패인가
     * @param retryAfter 상대가 알려 준 대기. 없으면 null
     */
    public Optional<Duration> next(int attempts, boolean retryable, Duration retryAfter) {
        if (!retryable || attempts < 1 || attempts > backoffs.size()) {
            return Optional.empty();
        }
        Duration base = backoffs.get(attempts - 1);
        return Optional.of(retryAfter != null && !retryAfter.isNegative() && !retryAfter.isZero() ? retryAfter : base);
    }
}
