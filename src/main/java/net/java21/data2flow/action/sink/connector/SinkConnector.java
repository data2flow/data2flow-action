package net.java21.data2flow.action.sink.connector;

import java.util.List;

/**
 * Sink 커넥터 SPI(FLW-04.02·04.05, TC-FLW-087). 저장소 종류마다 구현 하나를 두고, 플로우 Sink 노드는 연결 ID만 알며 종류를 모른다.
 * 새 저장소 종류는 이 인터페이스 구현 하나를 추가하고 계약 키트({@code SinkConnectorContractTest})를 통과시킨 뒤
 * {@link SinkConnectorVerified}를 붙여 등록한다. 키트를 거치지 않은 구현은 {@link SinkConnectorRegistry}가 거부한다.
 *
 * <p>계약:
 * <ul>
 *   <li>{@link #write}는 같은 {@link SinkBatch#idempotencyKey()}로 다시 불려도 대상에 한 번만 반영한다(NFR-02.11, BR-FLW-13).</li>
 *   <li>실패는 {@link SinkWriteException}으로 알리고, 다시 해 볼 만한 장애(연결·시간 초과·5xx)인지 표시한다.</li>
 *   <li>{@link #test}는 예외를 던지지 않고 원인 종류(AUTH·DNS·TLS·TIMEOUT·REFUSED·OTHER)로 돌려준다(API-FLW-51).</li>
 * </ul>
 */
public interface SinkConnector {

    /** 종류(core {@code sink_connections.type}, 예: POSTGRESQL) */
    String type();

    /** 연결 확인. 예외 없음 */
    SinkTestResult test(SinkConnection connection);

    /** 대상(테이블·measurement) 스키마(FLW-04.04) */
    TargetSchema describe(SinkConnection connection, String target) throws SinkWriteException;

    /** 대상 자동 생성(FLW-04.04 [자동 생성]). 이미 있으면 그대로 둔다 */
    void create(SinkConnection connection, String target, List<TargetSchema.Column> columns, List<String> primaryKey)
            throws SinkWriteException;

    /** 배치 하나를 쓴다. 이미 쓴 배치면 {@link WriteOutcome#ALREADY_WRITTEN} */
    WriteOutcome write(SinkConnection connection, SinkBatch batch) throws SinkWriteException;

    /** 연결이 바뀌거나 지워졌을 때 풀을 닫는다 */
    default void release(long connectionId) {
    }

    /** 종료 때 모든 풀을 닫는다 */
    default void close() {
    }

    enum WriteOutcome { WRITTEN, ALREADY_WRITTEN }
}
