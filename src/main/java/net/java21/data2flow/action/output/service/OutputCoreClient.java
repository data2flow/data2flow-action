package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.domain.DeviceContext;
import net.java21.data2flow.action.output.domain.OutputConnection;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 출력 연결용 core-api 내부 API(ADR-021, {@code X-CALLER-SERVICE: data2flow-action}).
 * <ul>
 *   <li>API-DSC-73 {@code GET /internal/core/output-connections/runtime?sinceVersion=}: 배포 조직의 출력 연결 전체(비밀값 복호화). 같으면 204</li>
 *   <li>API-DSC-74 {@code GET /internal/core/output-connections/device-contexts?deviceIds=}: 기기 이름·공간 코드·공간 경로·그룹</li>
 *   <li>API-DSC-75 {@code POST /internal/core/output-connections/stats}: 1분 발송 지표</li>
 * </ul>
 * 비밀값이 든 응답은 로그에 남기지 않는다.
 */
public class OutputCoreClient {

    private static final String CALLER = "data2flow-action";
    private final RestClient core;

    public OutputCoreClient(RestClient core) {
        this.core = core;
    }

    /** 연결 정의 묶음 */
    public record Runtime(long version, List<OutputConnection> connections) {
    }

    /** API-DSC-73. 바뀌지 않았으면(204) 빈 값 */
    public Optional<Runtime> runtime(Long sinceVersion) {
        return core.get().uri(b -> b.path("/internal/core/output-connections/runtime").queryParamIfPresent("sinceVersion",
                        Optional.ofNullable(sinceVersion)).build())
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> {
                    int status = res.getStatusCode().value();
                    if (status == 204) {
                        return Optional.<Runtime>empty();
                    }
                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("core API-DSC-73 실패: HTTP " + status);
                    }
                    JsonNode r = Json.MAPPER.readTree(res.getBody().readAllBytes()).path("response");
                    List<OutputConnection> list = new ArrayList<>();
                    r.path("connections").forEach(n -> list.add(OutputConnection.parse(n)));
                    return Optional.of(new Runtime(r.path("version").asLong(0), list));
                });
    }

    /** API-DSC-74. 모르는 기기는 빠진다 */
    public List<DeviceContext> deviceContexts(Collection<Long> deviceIds) {
        if (deviceIds.isEmpty()) {
            return List.of();
        }
        String ids = deviceIds.stream().map(String::valueOf).collect(Collectors.joining(","));
        JsonNode body = core.get().uri(b -> b.path("/internal/core/output-connections/device-contexts").queryParam("deviceIds", ids).build())
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .exchange((req, res) -> {
                    int status = res.getStatusCode().value();
                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("core API-DSC-74 실패: HTTP " + status);
                    }
                    return Json.MAPPER.readTree(res.getBody().readAllBytes());
                });
        List<DeviceContext> out = new ArrayList<>();
        body.path("response").path("devices").forEach(n -> out.add(DeviceContext.parse(n)));
        return out;
    }

    /** API-DSC-75 1분 지표 */
    public void postStats(List<Map<String, Object>> items) {
        core.post().uri("/internal/core/output-connections/stats")
                .header(DataflowHeaders.CALLER_SERVICE, CALLER)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Json.write(Map.of("items", items)))
                .exchange((req, res) -> {
                    int status = res.getStatusCode().value();
                    if (status < 200 || status >= 300) {
                        throw new IllegalStateException("core API-DSC-75 실패: HTTP " + status);
                    }
                    return null;
                });
    }
}
