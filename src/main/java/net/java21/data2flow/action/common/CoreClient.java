package net.java21.data2flow.action.common;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.contracts.authz.AccessGrant;
import net.java21.data2flow.contracts.authz.Permission;
import net.java21.data2flow.contracts.authz.SpaceScope;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * core-api 내부 API 호출(ADR-021: 토큰 없음, {@code X-CALLER-SERVICE: data2flow-action}).
 *
 * <ul>
 *   <li>API-ACT-40 {@code GET /internal/core/devices/{device-id}/control-profile}: 제어 프로필(기기·모델 기능·제약·보호·드라이버·조직 설정)</li>
 *   <li>API-ACT-41 {@code GET /internal/core/sim/sandbox-spaces}: 샌드박스 공간(하위 포함 펼침, BR-ACT-23)</li>
 *   <li>API-DEV-128 {@code GET /internal/core/spaces/{space-id}/devices?relation=CONTROLS&capability=&includeDescendants=}: 관계 대상 펼치기</li>
 *   <li>{@code GET /internal/core/organizations/{organization-id}/users/{user-id}/access-grant}: 권한 판정(IAM-04.01)</li>
 *   <li>API-ACT-44 {@code GET /internal/core/devices/{device-id}/interlocks}: 기기에 걸리는 인터락(BR-ACT-11)</li>
 *   <li>API-ACT-45 {@code GET /internal/core/metric-values?metric=&deviceId=|spaceId=&at=}: 측정값(인터락·효과 확인)</li>
 *   <li>API-ACT-46 {@code GET /internal/core/emergency-stops?active=true}: 진행 중 비상 정지(BR-ACT-12)</li>
 *   <li>API-ACT-47 {@code GET /internal/core/scenes/{scene-id}}: 장면 정의(BR-ACT-16)</li>
 * </ul>
 */
public class CoreClient {

    private static final String CALLER = "data2flow-action";
    private final RestClient core;

    public CoreClient(RestClient core) {
        this.core = core;
    }

    /** 제어 프로필. 기기가 없으면 빈 값 */
    public Optional<ControlProfile> controlProfile(long deviceId) {
        JsonNode body = get("/internal/core/devices/{id}/control-profile", deviceId);
        if (body == null) {
            return Optional.empty();
        }
        return Optional.of(Json.MAPPER.treeToValue(body.path("response"), ControlProfile.class));
    }

    /** 샌드박스 공간 ID(하위 공간 포함) */
    public Set<Long> sandboxSpaces() {
        JsonNode body = get("/internal/core/sim/sandbox-spaces");
        Set<Long> out = new LinkedHashSet<>();
        if (body != null) {
            body.path("response").path("spaceIds").forEach(n -> out.add(n.asLong()));
        }
        return out;
    }

    /** 공간 관계 대상 펼치기(관계 이름은 core 값 MEASURES·CONTROLS) */
    public List<Long> spaceDevices(long spaceId, String relation, String capability, boolean includeDescendants) {
        JsonNode body = core.get().uri(b -> b.path("/internal/core/spaces/{id}/devices")
                        .queryParam("relation", relation == null ? null : relation.toUpperCase(Locale.ROOT))
                        .queryParam("capability", capability)
                        .queryParam("includeDescendants", includeDescendants)
                        .build(spaceId))
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
        List<Long> out = new ArrayList<>();
        if (body != null) {
            body.path("response").forEach(n -> out.add(n.path("deviceId").asLong()));
        }
        return out;
    }

    /** 사용자 권한(없는 사용자·비활성·다른 조직이면 권한 없음) */
    public AccessGrant accessGrant(long organizationId, long userId) {
        JsonNode body = get("/internal/core/organizations/{org}/users/{user}/access-grant", organizationId, userId);
        if (body == null) {
            return AccessGrant.none();
        }
        JsonNode r = body.path("response");
        if (!r.path("active").asBoolean(false)) {
            return AccessGrant.none();
        }
        Set<Permission> permissions = EnumSet.noneOf(Permission.class);
        r.path("permissions").forEach(p -> {
            try {
                permissions.add(Permission.valueOf(p.asString()));
            } catch (IllegalArgumentException ignored) {
                // 이 코드보다 새 권한 이름은 무시(기본 거부)
            }
        });
        JsonNode scope = r.path("spaceScope");
        SpaceScope spaceScope;
        if (scope.path("unrestricted").asBoolean(true)) {
            spaceScope = SpaceScope.all();
        } else {
            Set<Long> ids = new LinkedHashSet<>();
            scope.path("allowedSpaceIds").forEach(n -> ids.add(Long.parseLong(n.asString())));
            spaceScope = SpaceScope.only(ids);
        }
        return new AccessGrant(r.path("role").asString("CUSTOM"), permissions, spaceScope);
    }

    /** API-ACT-44 기기에 걸리는 인터락(켜진 것, 공간 범위는 core가 판정) */
    public List<net.java21.data2flow.action.actuation.domain.Interlock> interlocks(long deviceId) {
        JsonNode body = get("/internal/core/devices/{id}/interlocks", deviceId);
        List<net.java21.data2flow.action.actuation.domain.Interlock> out = new ArrayList<>();
        if (body != null) {
            body.path("responses").forEach(n -> out.add(Json.MAPPER.treeToValue(n, net.java21.data2flow.action.actuation.domain.Interlock.class)));
        }
        return out;
    }

    /** API-ACT-45 측정값(at 이전 마지막). 기기 또는 공간(평균). 없으면 빈 값 */
    public Optional<net.java21.data2flow.action.actuation.domain.MetricValue> metricValue(String metric, Long deviceId, Long spaceId,
                                                                                          java.time.Instant at) {
        JsonNode body = core.get().uri(b -> {
                    b.path("/internal/core/metric-values").queryParam("metric", metric);
                    if (deviceId != null) {
                        b.queryParam("deviceId", deviceId);
                    }
                    if (spaceId != null) {
                        b.queryParam("spaceId", spaceId);
                    }
                    if (at != null) {
                        b.queryParam("at", at.toString());
                    }
                    return b.build();
                })
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
        if (body == null || body.path("response").isMissingNode() || body.path("response").isNull()) {
            return Optional.empty();
        }
        return Optional.of(Json.MAPPER.treeToValue(body.path("response"), net.java21.data2flow.action.actuation.domain.MetricValue.class));
    }

    /** API-ACT-46 진행 중 비상 정지(배포 조직 전체) */
    public List<net.java21.data2flow.action.actuation.domain.EmergencyStop> activeEmergencyStops() {
        JsonNode body = get("/internal/core/emergency-stops?active=true");
        List<net.java21.data2flow.action.actuation.domain.EmergencyStop> out = new ArrayList<>();
        if (body != null) {
            body.path("responses").forEach(n -> out.add(Json.MAPPER.treeToValue(n, net.java21.data2flow.action.actuation.domain.EmergencyStop.class)));
        }
        return out;
    }

    /** API-ACT-47 장면 정의. 없으면 빈 값 */
    public Optional<net.java21.data2flow.action.actuation.domain.SceneDefinition> scene(long sceneId) {
        JsonNode body = get("/internal/core/scenes/{id}", sceneId);
        return body == null ? Optional.empty()
                : Optional.of(Json.MAPPER.treeToValue(body.path("response"), net.java21.data2flow.action.actuation.domain.SceneDefinition.class));
    }

    private JsonNode get(String path, Object... vars) {
        return core.get().uri(path, vars)
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> res.getStatusCode().value() == 404 ? null : read(res.getStatusCode().value(), res.bodyTo(JsonNode.class)));
    }

    private static JsonNode read(int status, JsonNode body) {
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("core 내부 API 실패: HTTP " + status);
        }
        return body;
    }
}
