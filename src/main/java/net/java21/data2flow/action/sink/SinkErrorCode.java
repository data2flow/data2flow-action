package net.java21.data2flow.action.sink;

import net.java21.data2flow.contracts.error.ErrorCode;

/** Sink 오류 코드(FLW-api API-FLW-50·51). 문구는 messages-sink*.properties의 error.&lt;코드&gt;(ADR-037) */
public enum SinkErrorCode implements ErrorCode {
    SINK_CONNECTION_NOT_FOUND(404),
    SINK_CONNECTION_TEST_FAILED(502),
    SINK_TARGET_INVALID(400),
    SINK_TYPE_NOT_SUPPORTED(400),
    SINK_DEAD_LETTER_NOT_FOUND(404);

    private final int httpStatus;

    SinkErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    @Override
    public String code() {
        return name();
    }

    @Override
    public int httpStatus() {
        return httpStatus;
    }
}
