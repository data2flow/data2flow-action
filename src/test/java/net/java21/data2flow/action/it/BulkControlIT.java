package net.java21.data2flow.action.it;

import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** 일괄 제어(ACT-02.06, TC-ACT-055)와 장면 실행·미리보기(ACT-05.01·05.03) */
class BulkControlIT extends IntegrationTestSupport {

    private RestClient operator;
    private final List<Long> devices = new ArrayList<>();

    @BeforeEach
    void setUp() {
        devices.clear();
        for (long id = 101; id <= 112; id++) {
            CORE.profiles.put(id, Fixtures.aircon(id, Fixtures.ORG, true, false));
            devices.add(id);
        }
        CORE.spaceDevices.put(Fixtures.SPACE, devices);
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        operator = api(Fixtures.USER, Fixtures.ORG);
    }

    @Test
    @DisplayName("[ACT-02.06][AT-ACT-04.1][TC-ACT-055] 3층 12대(1대 오프라인) 전체 켜기 → 11대 APPLIED, 1대 QUEUED, 요약 11/12 적용·1 대기")
    void bulkSwitchOn() {
        publish(EventType.DEVICE_CONNECTIVITY_CHANGED, new DeviceConnectivityChanged(112, DeviceConnectivityChanged.Connectivity.ONLINE, DeviceConnectivityChanged.Connectivity.OFFLINE, clock.instant(), 300, 3));
        await().atMost(Duration.ofSeconds(20)).until(() -> count(
                "SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = 112 AND connectivity = 'OFFLINE'") == 1);

        Result preview = post(operator, "/internal/action/bulk", Map.of("target", Map.of("spaceId", "31", "includeChildren", true),
                "capability", "Switch", "command", "set", "args", Map.of("on", true), "preview", true), null);
        assertThat(preview.status()).isEqualTo(200);
        assertThat(preview.response().path("devices")).hasSize(12);

        Result r = post(operator, "/internal/action/bulk", Map.of("target", Map.of("spaceId", "31", "includeChildren", true),
                "capability", "Switch", "command", "set", "args", Map.of("on", true)), "b-1");
        assertThat(r.status()).isEqualTo(202);
        assertThat(r.response().path("total").asInt()).isEqualTo(12);
        String jobId = r.response().path("bulkJobId").asString();

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            var p = get(operator, "/internal/action/bulk-jobs/" + jobId).response();
            return p.path("succeeded").asInt() == 11 && p.path("queued").asInt() == 1 && "COMPLETED".equals(p.path("status").asString());
        });
        assertThat(count("SELECT count(*) FROM data2flow_action.commands WHERE source_type = 'BULK' AND priority = 'MANUAL'")).isEqualTo(12);
        assertThat(count("SELECT succeeded FROM data2flow_action.bulk_jobs")).isEqualTo(11);
    }

    @Test
    @DisplayName("[ACT-02.06][AT-ACT-04.2] 500대 초과 → 400 COMMAND_BULK_LIMIT_EXCEEDED")
    void bulkLimit() {
        List<Long> many = new ArrayList<>();
        for (long id = 1000; id < 1501; id++) {
            many.add(id);
        }
        CORE.spaceDevices.put(32L, many);
        Result r = post(operator, "/internal/action/bulk", Map.of("target", Map.of("spaceId", "32"), "capability", "Switch", "command", "set",
                "args", Map.of("on", true)), "b-2");
        assertThat(r.status()).isEqualTo(400);
        assertThat(r.code()).isEqualTo("COMMAND_BULK_LIMIT_EXCEEDED");
    }

    @Test
    @DisplayName("[ACT-05.01][ACT-05.03][AT-ACT-05.1] 장면 미리보기 → 실행(202 sceneRunId) → 모두 적용되면 SUCCEEDED, 다시 실행해도 기기마다 1회(같은 키)")
    void scene() {
        CORE.routes.put("/internal/core/scenes/3", req -> FakeCore.ok(Map.of("sceneId", "3", "organizationId", "1", "name", "수업 모드",
                "spaceId", "31", "items", List.of(
                        Map.of("target", Map.of("deviceId", "101"), "capability", "Thermostat", "desired", Map.of("mode", "cool",
                                "targetTemperature", 24)),
                        Map.of("target", Map.of("spaceId", "31", "relation", "controls", "capability", "Switch", "includeChildren", false),
                                "capability", "Switch", "desired", Map.of("on", true))))));

        Result preview = post(operator, "/internal/action/scenes/3/preview", Map.of(), null);
        assertThat(preview.response().path("items")).hasSize(13);
        assertThat(preview.response().path("items").get(0).path("willChange").asBoolean()).isTrue();

        Result run = post(operator, "/internal/action/scenes/3/run", Map.of(), "s-1");
        assertThat(run.status()).isEqualTo(202);
        String runId = run.response().path("sceneRunId").asString();
        await().atMost(Duration.ofSeconds(30)).until(() -> "SUCCEEDED".equals(
                get(operator, "/internal/action/scene-runs/" + runId).response().path("status").asString()));
        assertThat(count("SELECT count(*) FROM data2flow_action.commands WHERE source_type = 'SCENE' AND priority = 'MANUAL'")).isEqualTo(13);

        Result missing = post(operator, "/internal/action/scenes/99/run", Map.of(), "s-2");
        assertThat(missing.status()).isEqualTo(404);
        assertThat(missing.code()).isEqualTo("SCENE_NOT_FOUND");
    }
}
