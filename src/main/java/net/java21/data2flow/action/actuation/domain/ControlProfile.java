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
 * @param spacePathIds   루트부터 기기 공간까지 공간 ID(비상 정지 범위 판정 BR-ACT-12, ADR-049). 없으면 빈 목록
 * @param reportIntervalSec 기기 보고 주기(초). LoRaWAN Class A 예상 전달 시각(ACT-07.02)·인터락 데이터 없음 판정(BR-ACT-11). 없으면 null
 * @param ratedPowerW    모델 정격 전력(W). 가동 집계의 추정 에너지(BR-ACT-21). 없으면 null
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ControlProfile(long deviceId, long organizationId, Long spaceId, String name, String externalId, boolean virtual,
                             String status, Long modelId, Map<String, ModelCapability> capabilities, DriverBinding driver,
                             ControlSettings settings, java.util.List<Long> spacePathIds, Integer reportIntervalSec, Double ratedPowerW) {

    public static final String STATUS_ACTIVE = "ACTIVE";

    public ControlProfile {
        capabilities = capabilities == null ? Map.of() : java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(capabilities));
        settings = settings == null ? ControlSettings.DEFAULT : settings;
        status = status == null ? STATUS_ACTIVE : status;
        spacePathIds = spacePathIds == null || spacePathIds.isEmpty()
                ? (spaceId == null ? java.util.List.of() : java.util.List.of(spaceId)) : java.util.List.copyOf(spacePathIds);
    }

    /** M3 모양(공간 경로·보고 주기·정격 전력 없음) */
    public ControlProfile(long deviceId, long organizationId, Long spaceId, String name, String externalId, boolean virtual,
                          String status, Long modelId, Map<String, ModelCapability> capabilities, DriverBinding driver,
                          ControlSettings settings) {
        this(deviceId, organizationId, spaceId, name, externalId, virtual, status, modelId, capabilities, driver, settings, null, null, null);
    }

    /** 보고 주기 사본(시험·기본값) */
    public ControlProfile withReportInterval(Integer seconds) {
        return new ControlProfile(deviceId, organizationId, spaceId, name, externalId, virtual, status, modelId, capabilities, driver,
                settings, spacePathIds, seconds, ratedPowerW);
    }

    /** 제어할 수 있는 기기인지: 기능과 드라이버가 있고 활성 상태 */
    public boolean controllable() {
        return driver != null && !capabilities.isEmpty() && STATUS_ACTIVE.equals(status);
    }

    public Optional<ModelCapability> capability(String name) {
        return Optional.ofNullable(capabilities.get(name));
    }
}
