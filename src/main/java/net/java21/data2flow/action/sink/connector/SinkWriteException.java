package net.java21.data2flow.action.sink.connector;

/**
 * Sink 쓰기·조회 실패. {@code transientFailure}면 다시 시도하고(연결·시간 초과·5xx), 아니면 dead-letter로 보낸다(BR-FLW-28).
 */
public class SinkWriteException extends Exception {

    private final ErrorKind kind;
    private final boolean transientFailure;

    public SinkWriteException(ErrorKind kind, boolean transientFailure, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
        this.transientFailure = transientFailure;
    }

    public SinkWriteException(ErrorKind kind, boolean transientFailure, String message) {
        this(kind, transientFailure, message, null);
    }

    public ErrorKind kind() {
        return kind;
    }

    public boolean transientFailure() {
        return transientFailure;
    }
}
