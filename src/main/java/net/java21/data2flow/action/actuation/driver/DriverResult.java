package net.java21.data2flow.action.actuation.driver;

import java.util.Map;

/**
 * {@link DeviceDriver#execute} 결과.
 *
 * @param status    ACCEPTED(응답은 이벤트로), ACKED(호출 응답이 곧 ack), FAILED
 * @param reason    FAILED 사유(CommandStatusReasons, 예: DRIVER_ERROR·INVALID_COMMAND·DEVICE_NOT_SIMULATED)
 * @param retryable 일시 장애라 다시 부르면 될 수 있음(BR-ACT-14)
 * @param detail    벤더 응답 요약(commands.driver_response)
 */
public record DriverResult(Status status, String reason, boolean retryable, Map<String, Object> detail) {

    public enum Status { ACCEPTED, ACKED, FAILED }

    public DriverResult {
        detail = detail == null ? Map.of() : Map.copyOf(detail);
    }

    public static DriverResult accepted() {
        return new DriverResult(Status.ACCEPTED, null, false, Map.of());
    }

    public static DriverResult acked() {
        return new DriverResult(Status.ACKED, null, false, Map.of());
    }

    public static DriverResult failed(String reason, boolean retryable, String message) {
        return new DriverResult(Status.FAILED, reason, retryable, message == null ? Map.of() : Map.of("message", message));
    }
}
