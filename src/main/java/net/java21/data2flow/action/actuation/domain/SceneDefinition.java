package net.java21.data2flow.action.actuation.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * 장면 정의(core {@code scenes}·{@code scene_items}, API-ACT-47). 실행 기록({@code scene_runs})만 action이 갖는다.
 *
 * @param sceneId        장면 ID
 * @param organizationId 조직
 * @param name           이름
 * @param spaceId        장면 공간(샌드박스 판정 출처 공간). 없으면 null
 * @param items          항목(≤ 100, BR-ACT-16)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SceneDefinition(long sceneId, long organizationId, String name, Long spaceId, List<Item> items) {

    public SceneDefinition {
        items = items == null ? List.of() : List.copyOf(items);
    }

    /**
     * 항목.
     *
     * @param target     {@code {deviceId}} 또는 {@code {spaceId, relation, capability, includeChildren}}
     * @param capability 기능
     * @param desired    목표 상태(명령 set 인자)
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Item(Target target, String capability, Map<String, Object> desired) {
        public Item {
            desired = desired == null ? Map.of() : Map.copyOf(desired);
        }
    }

    /** 대상 */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Target(Long deviceId, Long spaceId, String relation, String capability, Boolean includeChildren) {
    }
}
