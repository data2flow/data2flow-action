package net.java21.data2flow.action.actuation.driver;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * 드라이버에 넘기는 표준 명령(ACT-api §5.3).
 *
 * @param commandId      명령 ID. 드라이버·장비는 이것으로 중복을 거른다
 * @param device         대상 기기
 * @param capability     기능
 * @param command        명령(set)
 * @param args           인자(목표 상태, BR-ACT-03)
 * @param validUntil     이 시각 뒤에는 적용하지 않는다
 * @param idempotencyKey 명령의 멱등 키
 * @param desiredVersion 이 명령으로 정해진 desired 버전(가상 장비 API-SIM-30 기록용)
 */
public record DriverCommand(UUID commandId, DriverDevice device, String capability, String command, Map<String, Object> args,
                            Instant validUntil, String idempotencyKey, long desiredVersion) {

    public DriverCommand {
        args = args == null ? Map.of() : Map.copyOf(args);
    }
}
