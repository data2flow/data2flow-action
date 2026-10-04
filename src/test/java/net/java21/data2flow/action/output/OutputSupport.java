package net.java21.data2flow.action.output;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.output.service.DeviceContextCache;
import net.java21.data2flow.action.output.service.OutputConnectionRegistry;
import net.java21.data2flow.action.support.FakeCore;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import okhttp3.mockwebserver.MockResponse;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 출력 연결 시험 기반: 가짜 core에 API-DSC-73(연결 정의)·74(기기 맥락)·75(지표)를 더하고, 외부 대상으로 Testcontainers Mosquitto를 띄운다
 * (1883 익명 허용, 1884 인증 실패용). 공용 브로커에는 붙지 않는다(CLAUDE.md §5).
 */
public abstract class OutputSupport extends IntegrationTestSupport {

    @SuppressWarnings("resource")
    protected static final GenericContainer<?> MOSQUITTO = new GenericContainer<>("eclipse-mosquitto:2.0")
            .withExposedPorts(1883, 1884)
            .withCopyToContainer(Transferable.of("""
                    per_listener_settings true
                    listener 1883
                    allow_anonymous true
                    listener 1884
                    allow_anonymous false
                    """), "/mosquitto/config/mosquitto.conf")
            .waitingFor(Wait.forListeningPort());

    static {
        MOSQUITTO.start();
    }

    /** core가 돌려줄 출력 연결(API-DSC-73 항목 모양) */
    protected final List<Map<String, Object>> connections = new CopyOnWriteArrayList<>();
    /** core가 받은 지표 항목(API-DSC-75) */
    protected final List<JsonNode> statsItems = new CopyOnWriteArrayList<>();
    private final AtomicLong version = new AtomicLong();

    @Autowired
    protected OutputConnectionRegistry registry;
    @Autowired
    protected DeviceContextCache contexts;
    @Autowired
    protected net.java21.data2flow.action.output.service.OutputStats outputStats;

    @BeforeEach
    void outputCore() {
        connections.clear();
        statsItems.clear();
        CORE.routes.put("/internal/core/output-connections/runtime", r -> {
            long v = version.incrementAndGet();
            return FakeCore.ok(Map.of("version", v, "connections", List.copyOf(connections)));
        });
        CORE.routes.put("/internal/core/output-connections/device-contexts",
                r -> FakeCore.ok(Map.of("devices", List.of(OutputFixtures.contextJson()))));
        CORE.routes.put("/internal/core/output-connections/stats", r -> {
            Json.MAPPER.readTree(r.getBody().readUtf8()).path("items").forEach(statsItems::add);
            return new MockResponse().setResponseCode(204);
        });
        registry.invalidate();
        contexts.invalidateAll();
        outputStats.flush(true); // 앞 시험이 남긴 지표를 비운다
        statsItems.clear();
    }

    /** 연결 정의를 바꾸고 다음 호출에서 다시 읽게 한다(설정 변경 OUTPUT을 받은 것처럼) */
    protected void setConnections(List<Map<String, Object>> list) {
        connections.clear();
        connections.addAll(list);
        registry.invalidate();
    }

    protected static String mosquittoUrl() {
        return "mqtt://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1883);
    }

    protected static String mosquittoAuthUrl() {
        return "mqtt://" + MOSQUITTO.getHost() + ":" + MOSQUITTO.getMappedPort(1884);
    }
}
