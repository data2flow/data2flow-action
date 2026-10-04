package net.java21.data2flow.action.output;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * 출력 연결 설정({@code data2flow.action.output.*}, DSC-04.01·BR-DSC-19).
 *
 * @param consumerEnabled  {@code data2flow.telemetry} 소비(그룹 action-output). prod·staging 켬, local·시험 끔(스트림 IT만 켬)
 * @param senderEnabled    주기 발송·정리·지표 전송 작업(시험은 끄고 직접 부른다)
 * @param developer        로컬 소비자 그룹 접미사(action-output-&lt;이름&gt;, ADR-030)
 * @param streamPort       RabbitMQ Stream 포트(5552, 내부망 전용)
 * @param streamFixedAddress 브로커가 알려 주는 주소 대신 설정 주소로만 접속(단일 노드·시험)
 * @param runtimeRefresh   연결 정의 다시 읽기 주기(API-DSC-73, 30초. 설정 변경 OUTPUT이 오면 즉시)
 * @param contextTtl       기기 맥락 캐시(API-DSC-74, 60초)
 * @param senderPeriod     발송 작업 주기
 * @param retryInitial     첫 재시도 간격(1초)
 * @param retryMax         최대 재시도 간격(5분)
 * @param maxRetryAge      다시 시도하는 기간(24시간). 지나면 FAILED(실패 보관함)
 * @param lease            연결 발송 리스(파드 하나만 보내 순서 유지)
 * @param batch            한 번에 꺼내는 최대 행 수(MQTT·Webhook 배치 상한)
 * @param retention        보낸·실패 행 보관(7일)
 * @param testTimeout      연결 테스트·발송 제한 시간(10초)
 * @param deniedHosts      추가 접속 금지 주소(공용 브로커 iot-data.java21.net은 항상 금지)
 */
@ConfigurationProperties(prefix = "data2flow.action.output")
public record OutputProperties(boolean consumerEnabled, Boolean senderEnabled, String developer, int streamPort,
                               Boolean streamFixedAddress, Duration runtimeRefresh, Duration contextTtl, Duration senderPeriod,
                               Duration retryInitial, Duration retryMax, Duration maxRetryAge, Duration lease, int batch,
                               Duration retention, Duration testTimeout, List<String> deniedHosts) {

    public OutputProperties {
        senderEnabled = senderEnabled == null || senderEnabled;
        streamPort = streamPort <= 0 ? 5552 : streamPort;
        streamFixedAddress = streamFixedAddress == null || streamFixedAddress;
        runtimeRefresh = runtimeRefresh == null ? Duration.ofSeconds(30) : runtimeRefresh;
        contextTtl = contextTtl == null ? Duration.ofSeconds(60) : contextTtl;
        senderPeriod = senderPeriod == null ? Duration.ofMillis(500) : senderPeriod;
        retryInitial = retryInitial == null ? Duration.ofSeconds(1) : retryInitial;
        retryMax = retryMax == null ? Duration.ofMinutes(5) : retryMax;
        maxRetryAge = maxRetryAge == null ? Duration.ofHours(24) : maxRetryAge;
        lease = lease == null ? Duration.ofSeconds(60) : lease;
        batch = batch <= 0 ? 100 : Math.min(batch, 500);
        retention = retention == null ? Duration.ofDays(7) : retention;
        testTimeout = testTimeout == null ? Duration.ofSeconds(10) : testTimeout;
        deniedHosts = deniedHosts == null ? List.of() : List.copyOf(deniedHosts);
    }

    public static OutputProperties defaults() {
        return new OutputProperties(false, null, null, 0, null, null, null, null, null, null, null, null, 0, null, null, null);
    }
}
