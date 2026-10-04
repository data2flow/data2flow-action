package net.java21.data2flow.action.output.domain;

import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 출력 필터·토픽에 쓰는 기기 맥락(core API-DSC-74 항목): 이름, 공간 코드, 공간 경로(루트부터 자기까지, 하위 공간 포함 판정), 그룹.
 */
public record DeviceContext(long deviceId, long organizationId, String deviceName, Long spaceId, String spaceCode, List<Long> spacePathIds,
                            Set<Long> groupIds) {

    public DeviceContext {
        spacePathIds = spacePathIds == null ? List.of() : List.copyOf(spacePathIds);
        groupIds = groupIds == null ? Set.of() : Set.copyOf(groupIds);
    }

    /** 맥락이 없을 때(기기 정보를 못 받음): 기기 ID만 */
    public static DeviceContext unknown(long deviceId, long organizationId) {
        return new DeviceContext(deviceId, organizationId, null, null, null, List.of(), Set.of());
    }

    public static DeviceContext parse(JsonNode n) {
        List<Long> path = new ArrayList<>();
        n.path("spacePathIds").forEach(x -> path.add(OutputConnection.id(x)));
        Set<Long> groups = new LinkedHashSet<>();
        n.path("groupIds").forEach(x -> groups.add(OutputConnection.id(x)));
        JsonNode space = n.path("spaceId");
        return new DeviceContext(OutputConnection.id(n.path("deviceId")), OutputConnection.id(n.path("organizationId")),
                n.path("deviceName").isNull() ? null : n.path("deviceName").asString(null),
                space.isMissingNode() || space.isNull() ? null : OutputConnection.id(space),
                n.path("spaceCode").isNull() ? null : n.path("spaceCode").asString(null), path, groups);
    }
}
