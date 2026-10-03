package net.java21.data2flow.action.actuation.driver;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 등록된 드라이버(종류 → 구현). 계약 테스트 키트를 통과한 구현만 빈으로 둔다(BR-ACT-19). MQTT 드라이버는 설정으로 켰을 때만 있다.
 */
public class DriverRegistry {

    private final Map<String, DeviceDriver> byType;

    public DriverRegistry(List<DeviceDriver> drivers) {
        this.byType = drivers.stream().collect(Collectors.toUnmodifiableMap(DeviceDriver::type, Function.identity()));
    }

    public Optional<DeviceDriver> find(String type) {
        return type == null ? Optional.empty() : Optional.ofNullable(byType.get(type));
    }

    public Map<String, DeviceDriver> all() {
        return byType;
    }
}
