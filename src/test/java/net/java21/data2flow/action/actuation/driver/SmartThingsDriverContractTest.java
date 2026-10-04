package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.vendor.SmartThingsDriver;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.secret.Secret;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 계약 키트를 SmartThings 드라이버로(ACT-03.04, TC-ACT-075, ADR-040). MockWebServer가 공개 API 문서의 예시 요청·응답을 흉내 낸다.
 * 실제 벤더 API 호출 금지(키 없음).
 */
class SmartThingsDriverContractTest extends DriverContractTest {

    /** 공개 문서 예시 모양의 기기 상태 */
    static final String STATUS = """
            {"components":{"main":{
              "switch":{"switch":{"value":"on"}},
              "thermostatMode":{"thermostatMode":{"value":"cool"}},
              "thermostatCoolingSetpoint":{"coolingSetpoint":{"value":24,"unit":"C"}},
              "temperatureMeasurement":{"temperature":{"value":26.5,"unit":"C"}},
              "switchLevel":{"level":{"value":80}},
              "lock":{"lock":{"value":"locked"}}}}}""";

    private static final VendorApiFake API = new VendorApiFake("st-token", "X-Request-Id", id -> id);
    private static final RecordingSink SINK = new RecordingSink();
    private static final MutableClock CLOCK = new MutableClock(MutableClock.T0);
    private static final SmartThingsDriver DRIVER = new SmartThingsDriver(API.url(), Duration.ofSeconds(2), SINK, CLOCK);
    private static final DriverConfig CONFIG = new DriverConfig(15L, "SMARTTHINGS", Map.of("locationId", "loc-1"),
            Map.of("token", Secret.of("st-token")));
    private static final DriverDevice DEVICE = new DriverDevice(15, 1, "st-dev-1", false, CONFIG);

    static {
        API.stateBody = STATUS;
        API.commandResponse = "{\"results\":[{\"id\":\"c-1\",\"status\":\"ACCEPTED\"}]}";
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
                DRIVER.onEvent(1, 15, Json.MAPPER.readTree("""
                        {"lifecycle":"EVENT","eventData":{"events":[{"eventType":"DEVICE_EVENT","deviceEvent":{
                          "deviceId":"st-dev-1","componentId":"main","capability":"switch","attribute":"switch","value":"on"}}]}}"""));
            }

            @Override
            public void breakConnection() {
                API.mode = "ERROR";
            }
        };
    }

    @Test
    @DisplayName("[ACT-03.04][TC-ACT-075] 명령 매핑: Switch → switch.on, Thermostat → setThermostatMode + setCoolingSetpoint, Dimmer, Lock")
    void commandMapping() {
        DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Thermostat", "set", Map.of("mode", "cool", "targetTemperature", 24),
                null, "k", 1));
        var body = API.calls.get(API.calls.size() - 1).body().path("commands");
        assertThat(API.calls.get(API.calls.size() - 1).path()).isEqualTo("/v1/devices/st-dev-1/commands");
        assertThat(body.get(0).path("capability").asString()).isEqualTo("thermostatMode");
        assertThat(body.get(0).path("command").asString()).isEqualTo("setThermostatMode");
        assertThat(body.get(1).path("capability").asString()).isEqualTo("thermostatCoolingSetpoint");
        assertThat(body.get(1).path("arguments").get(0).asInt()).isEqualTo(24);
        DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Switch", "set", Map.of("on", false), null, "k", 1));
        assertThat(API.calls.get(API.calls.size() - 1).body().path("commands").get(0).path("command").asString()).isEqualTo("off");
        DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Dimmer", "set", Map.of("level", 40), null, "k", 1));
        assertThat(API.calls.get(API.calls.size() - 1).body().path("commands").get(0).path("command").asString()).isEqualTo("setLevel");
        DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Lock", "set", Map.of("locked", true), null, "k", 1));
        assertThat(API.calls.get(API.calls.size() - 1).body().path("commands").get(0).path("command").asString()).isEqualTo("lock");
        assertThat(DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Ventilation", "set", Map.of("mode", "on"), null, "k", 1))
                .reason()).isEqualTo("CAPABILITY_NOT_SUPPORTED");
        assertThat(DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Thermostat", "set", Map.of(), null, "k", 1))
                .reason()).isEqualTo("CAPABILITY_NOT_SUPPORTED");
    }

    @Test
    @DisplayName("[ACT-03.04][TC-ACT-075] 상태 응답·구독 이벤트 → 표준 상태 보고, 429는 일시 실패(백오프 재시도 대상)")
    void stateEventsAndRateLimit() {
        ReportedState s = DRIVER.getState(DEVICE).orElseThrow();
        assertThat(s.capabilities().get("Switch")).containsEntry("on", true);
        assertThat(s.capabilities().get("Thermostat")).containsEntry("mode", "cool").containsEntry("targetTemperature", 24.0)
                .containsEntry("currentTemperature", 26.5);
        assertThat(s.capabilities().get("Dimmer")).containsEntry("level", 80);
        assertThat(s.capabilities().get("Lock")).containsEntry("locked", true);
        SINK.reports.clear();
        assertThat(DRIVER.onEvent(1, 15, Json.MAPPER.readTree("""
                {"lifecycle":"EVENT","eventData":{"events":[
                  {"eventType":"DEVICE_EVENT","deviceEvent":{"capability":"switchLevel","attribute":"level","value":30}},
                  {"eventType":"MODE_EVENT"}]}}"""))).isEqualTo(1);
        assertThat(SINK.reports.get(0).capabilities().get("Dimmer")).containsEntry("level", 30);
        API.mode = "RATE_LIMITED";
        DriverResult limited = DRIVER.execute(new DriverCommand(UUID.randomUUID(), DEVICE, "Switch", "set", Map.of("on", true), null, "k", 1));
        assertThat(limited.status()).isEqualTo(DriverResult.Status.FAILED);
        assertThat(limited.retryable()).isTrue();
        API.mode = "OK";
        assertThat(DRIVER.healthCheck(new DriverConfig(15L, "SMARTTHINGS", Map.of(), Map.of("token", Secret.of("bad")))).errorKind())
                .isEqualTo("AUTH");
        assertErrorsAreFailedResults();
    }
}
