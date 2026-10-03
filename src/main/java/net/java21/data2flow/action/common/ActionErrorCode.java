package net.java21.data2flow.action.common;

import net.java21.data2flow.contracts.error.ErrorCode;

/** ACT 도메인 오류 코드(spec/detail/ACT/domain-model.md §5). 문구는 messages*.properties의 error.&lt;코드&gt;(ADR-037) */
public enum ActionErrorCode implements ErrorCode {
    DEVICE_NOT_FOUND(404),
    DEVICE_NOT_CONTROLLABLE(409),
    CAPABILITY_NOT_SUPPORTED(400),
    COMMAND_ARGS_INVALID(400),
    COMMAND_ARG_OUT_OF_RANGE(400),
    COMMAND_ABSOLUTE_LIMIT(400),
    COMMAND_RATE_LIMITED(429),
    COMMAND_BLOCKED(409),
    ACT_SANDBOX_FORBIDDEN(403),
    COMMAND_NOT_FOUND(404),
    COMMAND_NOT_CANCELLABLE(409),
    DRIVER_NOT_FOUND(404),
    DRIVER_HEALTHCHECK_FAILED(502);

    private final int httpStatus;

    ActionErrorCode(int httpStatus) {
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
