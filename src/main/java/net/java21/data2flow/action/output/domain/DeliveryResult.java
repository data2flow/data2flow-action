package net.java21.data2flow.action.output.domain;

import net.java21.data2flow.contracts.output.OutputFailureKind;

/**
 * 발송 결과. 실패면 {@code failureKind}와 사유, 성공이면 응답 상태(Webhook)·본문 일부.
 */
public record DeliveryResult(boolean ok, OutputFailureKind failureKind, String error, Integer status, String bodyPreview) {

    public static DeliveryResult success(Integer status, String bodyPreview) {
        return new DeliveryResult(true, null, null, status, bodyPreview);
    }

    public static DeliveryResult failure(OutputFailureKind kind, String error, Integer status, String bodyPreview) {
        return new DeliveryResult(false, kind, error == null ? kind.name() : error, status, bodyPreview);
    }
}
