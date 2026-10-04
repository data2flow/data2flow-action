package net.java21.data2flow.action.notification;

import net.java21.data2flow.contracts.error.ErrorCode;

/** 알림 오류 코드(OPS-06·RUL-03·05). 문구는 messages-notification*.properties의 error.&lt;코드&gt;(ADR-037) */
public enum NotificationErrorCode implements ErrorCode {
    CHANNEL_NOT_FOUND(404),
    CHANNEL_NOT_CONFIGURED(409),
    CHANNEL_TYPE_UNKNOWN(400),
    CHANNEL_TEST_FAILED(502),
    CHANNEL_WEBHOOK_UNSUPPORTED(409),
    MESSENGER_NOT_LINKED(403),
    CALLBACK_REJECTED(401),
    DELIVERY_NOT_FOUND(404),
    DELIVERY_NOT_RESENDABLE(409);

    private final int httpStatus;

    NotificationErrorCode(int httpStatus) {
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
