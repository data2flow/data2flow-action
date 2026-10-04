package net.java21.data2flow.action.sink.connector;

/**
 * 연결 테스트 결과(API-FLW-51 {@code {ok, latencyMs, error?:{kind, message}}}).
 *
 * @param ok        성공
 * @param latencyMs 걸린 시간
 * @param errorKind 실패 원인 종류. 성공이면 null
 * @param message   실패 설명(비밀값 없음). 성공이면 null
 */
public record SinkTestResult(boolean ok, long latencyMs, ErrorKind errorKind, String message) {

    public static SinkTestResult ok(long latencyMs) {
        return new SinkTestResult(true, latencyMs, null, null);
    }

    public static SinkTestResult failed(long latencyMs, ErrorKind kind, String message) {
        return new SinkTestResult(false, latencyMs, kind, message);
    }
}
