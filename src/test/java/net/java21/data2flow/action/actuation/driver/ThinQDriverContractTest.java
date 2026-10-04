package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.vendor.LgThinqDriver;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.secret.Secret;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 계약 키트를 LG ThinQ Connect 드라이버로(ACT-03.04, TC-ACT-074, ADR-040). MockWebServer가 공개 문서의 예시 요청·응답으로 벤더 REST API를
 * 흉내 낸다. 실제 벤더 API 호출 금지(키 없음).
 */
class ThinQDriverContractTest extends DriverContractTest {

    /** 공개 문서 예시 모양의 에어컨 상태 응답 */
    static final String STATE = """
            {"messageId":"fNvdZ1brTn-wWKUlWGoSVw","timestamp":"2026-03-02T00:00:00.000000","response":{
              "airConJobMode":{"currentJobMode":"COOL"},
              "operation":{"airConOperationMode":"POWER_ON"},
              "temperature":{"currentTemperature":26.5,"targetTemperature":24,"unit":"C"}}}""";

    private static final VendorApiFake API = new VendorApiFake("pat-1", "x-message-id", ThinQDriverContractTest::decode);
    private static final RecordingSink SINK = new RecordingSink();
    private static final MutableClock CLOCK = new MutableClock(MutableClock.T0);
    private static final LgThinqDriver DRIVER = new LgThinqDriver(API.url(), Duration.ofSeconds(2), SINK, CLOCK);
    private static final DriverConfig CONFIG = new DriverConfig(12L, "LG_THINQ", Map.of("country", "KR", "clientId", "data2flow-test",
            "apiKey", "doc-example-key"), Map.of("token", Secret.of("pat-1"), "refreshToken", Secret.of("rt-1")));
    private static final DriverDevice DEVICE = new DriverDevice(15, 1, "thinq-ac-1", false, CONFIG);

    static {
        API.stateBody = STATE;
        API.commandResponse = "{\"messageId\":\"x\",\"timestamp\":\"2026-03-02T00:00:00\",\"response\":{}}";
    }

    static String decode(String messageId) {
        ByteBuffer b = ByteBuffer.wrap(Base64.getUrlDecoder().decode(messageId));
        return new UUID(b.getLong(), b.getLong()).toString();
    }

    @Override
    protected DeviceDriver driver() {
        return DRIVER;
    }

    @Override
    protected DriverDevice device() {
        return DEVICE;
    }

    @Override
    protected RecordingSink sink() {
        return SINK;
    }

    @Override
    protected DevicePeer peer() {
        return new DevicePeer() {
            @Override
            public void respond(boolean respond) {
                API.mode = respond ? "OK" : "SILENT";
            }

            @Override
            public int effects(UUID commandId) {
                return API.effects(commandId.toString());
            }

            @Override
            public void emitState(Map<String, Map<String, Object>> capabilities) {
                // 푸시 통지(공개 문서의 report 모양)
                DRIVER.onPush(1, 15, Json.MAPPER.readTree("{\"deviceId\":\"thinq-ac-1\",\"report\":{\"operation\":{\"airConOperationMode\":"
                        + (Boolean.TRUE.equals(capabilities.getOrDefault("Switch", Map.of()).get("on")) ? "\"POWER_ON\"" : "\"POWER_OFF\"") + "}}}"));
            }

            @Override
            public void breakConnection() {
                API.mode = "ERROR";
            }
        };
    }

    @Test
    @DisplayName("[ACT-03.04][TC-ACT-074] 제어 요청 본문 매핑: 냉방 24℃ → airConJobMode COOL + targetTemperature 24, 끄기 → POWER_OFF, 헤더 포함")
    void controlMapping() {
        DriverResult r = DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Thermostat", "set", Map.of("mode", "cool",
                "targetTemperature", 24), null, "k", 1));
        assertThat(r.status()).isEqualTo(DriverResult.Status.ACKED);
        var call = API.calls.get(API.calls.size() - 1);
        assertThat(call.path()).isEqualTo("/devices/thinq-ac-1/control");
        assertThat(call.body().path("airConJobMode").path("currentJobMode").asString()).isEqualTo("COOL");
        assertThat(call.body().path("temperature").path("targetTemperature").asInt()).isEqualTo(24);
        DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Thermostat", "set", Map.of("mode", "off"), null, "k", 1));
        assertThat(API.calls.get(API.calls.size() - 1).body().path("operation").path("airConOperationMode").asString()).isEqualTo("POWER_OFF");
        assertThat(DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Thermostat", "set", Map.of("mode", "turbo"), null, "k", 1))
                .reason()).isEqualTo("CAPABILITY_NOT_SUPPORTED");
        assertThat(DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Dimmer", "set", Map.of("level", 1), null, "k", 1))
                .retryable()).isFalse();
    }

    @Test
    @DisplayName("[ACT-03.04][TC-ACT-074] 상태 조회 응답 → 표준 기능 상태(Switch on, Thermostat cool·24·26.5), 푸시 통지 → device.state.reported")
    void stateMapping() {
        ReportedState s = DRIVER.getState(DEVICE).orElseThrow();
        assertThat(s.capabilities().get("Switch")).containsEntry("on", true);
        assertThat(s.capabilities().get("Thermostat")).containsEntry("mode", "cool").containsEntry("targetTemperature", 24.0)
                .containsEntry("currentTemperature", 26.5);
        assertThat(s.version()).isPositive();
        SINK.reports.clear();
        assertThat(DRIVER.onPush(1, 15, Json.MAPPER.readTree("{\"report\":{\"operation\":{\"airConOperationMode\":\"POWER_OFF\"}}}"))).isTrue();
        assertThat(SINK.reports.get(0).capabilities().get("Thermostat")).containsEntry("mode", "off");
        assertThat(DRIVER.onPush(1, 15, Json.MAPPER.readTree("{\"report\":{}}"))).isFalse();
    }

    @Test
    @DisplayName("[ACT-03.04][TC-ACT-074] 토큰 만료 401 → 갱신 토큰으로 새 토큰을 받아 1회만 다시 보낸다, 429는 일시 실패(재시도)")
    void tokenRefreshAndRateLimit() {
        API.refreshedToken = "pat-2";
        API.accessToken = "pat-2";   // 기존 토큰 만료
        DriverResult r = DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(r.status()).isEqualTo(DriverResult.Status.ACKED);
        API.mode = "RATE_LIMITED";
        DriverResult limited = DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Switch", "set", Map.of("on", false), null, "k", 1));
        assertThat(limited.retryable()).isTrue();
        assertThat(limited.detail()).containsEntry("retryAfterSec", "7");
        API.mode = "OK";
        // 갱신도 실패하면 AUTH(재시도 없음)
        API.refreshedToken = null;
        API.accessToken = "pat-3";
        DriverConfig noRefresh = new DriverConfig(13L, "LG_THINQ", CONFIG.config(), Map.of("token", Secret.of("old")));
        DriverResult auth = DRIVER.execute(new DriverCommand(UUID.randomUUID(), new DriverDevice(15, 1, "thinq-ac-1", false, noRefresh),
                "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(auth.reason()).isEqualTo("AUTH");
        assertThat(DRIVER.healthCheck(noRefresh).errorKind()).isEqualTo("AUTH");
        API.accessToken = "pat-2";
        assertErrorsAreFailedResults();
        API.mode = "OK";
        DriverConfig unreachable = new DriverConfig(14L, "LG_THINQ", Map.of(), Map.of());
        assertThat(new LgThinqDriver("http://localhost:1", Duration.ofSeconds(1), SINK, CLOCK).healthCheck(unreachable).errorKind())
                .isEqualTo("UNREACHABLE");
        assertThat(new LgThinqDriver("http://localhost:1/", Duration.ofSeconds(1), SINK, CLOCK).getState(DEVICE)).isEmpty();
    }
}
