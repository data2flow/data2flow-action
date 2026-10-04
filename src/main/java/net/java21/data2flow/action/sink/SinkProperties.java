package net.java21.data2flow.action.sink;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Sink 설정({@code data2flow.action.sink.*}, FLW-04.03·BR-FLW-28).
 *
 * @param connectionTtl     연결 정의 캐시 최대 수명(설정 변경 메시지를 놓쳐도 이 시간 안에 다시 읽는다)
 * @param poolSize          연결마다 JDBC 풀 크기(작게, 대상 DB 보호)
 * @param connectTimeout    대상 연결 시간 제한
 * @param retryInitial      첫 재시도 간격(1초)
 * @param retryMax          최대 재시도 간격(60초)
 * @param maxRetryAge       일시 장애를 다시 시도하는 최대 기간(받은 시각부터, 기본 6시간). 지나면 dead-letter
 * @param deadRetention     dead-letter 보관(24시간)
 * @param writtenRetention  쓴 배치 행 보관(7일, 멱등은 executed_actions가 영구 보관)
 * @param lease             재시도 작업이 가져간 행을 다른 파드가 다시 잡지 않는 시간
 * @param batch             재시도 작업 한 번에 처리할 행 수
 */
@ConfigurationProperties(prefix = "data2flow.action.sink")
public record SinkProperties(Duration connectionTtl, int poolSize, Duration connectTimeout, Duration retryInitial, Duration retryMax,
                             Duration maxRetryAge, Duration deadRetention, Duration writtenRetention, Duration lease, int batch) {

    public SinkProperties {
        connectionTtl = connectionTtl == null ? Duration.ofMinutes(5) : connectionTtl;
        poolSize = poolSize <= 0 ? 2 : poolSize;
        connectTimeout = connectTimeout == null ? Duration.ofSeconds(5) : connectTimeout;
        retryInitial = retryInitial == null ? Duration.ofSeconds(1) : retryInitial;
        retryMax = retryMax == null ? Duration.ofSeconds(60) : retryMax;
        maxRetryAge = maxRetryAge == null ? Duration.ofHours(6) : maxRetryAge;
        deadRetention = deadRetention == null ? Duration.ofHours(24) : deadRetention;
        writtenRetention = writtenRetention == null ? Duration.ofDays(7) : writtenRetention;
        lease = lease == null ? Duration.ofSeconds(60) : lease;
        batch = batch <= 0 ? 50 : batch;
    }

    public static SinkProperties defaults() {
        return new SinkProperties(null, 0, null, null, null, null, null, null, null, 0);
    }
}
