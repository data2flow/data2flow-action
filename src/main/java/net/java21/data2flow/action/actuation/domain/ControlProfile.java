package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;
import java.util.Optional;

/**
 * 제어 창구가 명령 하나를 판단하는 데 필요한 기기 정보(core-api 내부 API {@code GET /internal/core/devices/{device-id}/control-profile},
 * ACT-api §5.4). 기능·모델 제약·드라이버 연결·조직 제어 설정의 정의는 core-api가 가지고 action은 캐시만 한다(ACT domain-model 머리말).
 *
 * @param deviceId       기기 ID
 * @param organizationId 조직
 * @param spaceId        기기의 공간(권한 범위·이벤트). 없으면 null
 * @param name           기기 이름(감사·이력 표시)
 * @param externalId     외부 ID(MQTT 드라이버의 device-key)
 * @param virtual        가상 기기(SIM-07.03 샌드박스 판정)
 * @param status         기기 상태(ACTIVE만 제어)
 * @param modelId        모델 ID
 * @param capabilities   모델이 지원하는 기능 → 모델 제약·보호(ACT-01.03, ACT-06.01)
 * @param driver         모델에 연결된 드라이버. 없으면 null(DEVICE_NOT_CONTROLLABLE)
 * @param settings       조직 제어 설정(API-ACT-17). 없으면 기본값
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ControlProfile(long deviceId, long organizationId, Long spaceId, String name, String externalId, boolean virtual,
                             String status, Long modelId, Map<String, ModelCapability> capabilities, DriverBinding driver,
                             ControlSettings settings) {

    public static final String STATUS_ACTIVE = "ACTIVE";

    public ControlProfile {
        capabilities = capabilities == null ? Map.of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(capabilities));
        settings = settings == null ? ControlSettings.DEFAULT : settings;
        status = status == null ? STATUS_ACTIVE : status;
    }

    /** 제어할 수 있는 기기인지: 기능과 드라이버가 있고 활성 상태 */
    public boolean controllable() {
        return driver != null && !capabilities.isEmpty() && STATUS_ACTIVE.equals(status);
    }

    public Optional<ModelCapability> capability(String name) {
        return Optional.ofNullable(capabilities.get(name));
    }
}
