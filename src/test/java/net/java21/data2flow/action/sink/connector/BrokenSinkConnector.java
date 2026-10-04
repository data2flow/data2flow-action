package net.java21.data2flow.action.sink.connector;

/**
 * 계약을 어기는 가짜 커넥터(멱등 무시: 같은 배치를 다시 쓰면 또 쓴다, 표시도 없음). 키트가 잡아내는지와 등록부가 거부하는지 확인한다.
 */
public class BrokenSinkConnector extends InMemorySinkConnector {

    @Override
    public synchronized WriteOutcome write(SinkConnection c, SinkBatch batch) throws SinkWriteException {
        written.remove(batch.idempotencyKey());
        return super.write(c, batch);
    }
}
