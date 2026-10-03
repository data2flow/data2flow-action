package net.java21.data2flow.action.actuation.driver;

/**
 * 드라이버가 보는 기기.
 *
 * @param deviceId       기기 ID
 * @param organizationId 조직
 * @param externalId     외부 ID(MQTT device-key, 벤더 기기 ID)
 * @param virtual        가상 기기
 * @param config         연결된 드라이버 설정
 */
public record DriverDevice(long deviceId, long organizationId, String externalId, boolean virtual, DriverConfig config) {
}
