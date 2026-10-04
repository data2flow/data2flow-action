package net.java21.data2flow.action.actuation.driver.vendor;

import net.java21.data2flow.action.actuation.driver.DriverCommand;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverEventSink;
import tools.jackson.databind.JsonNode;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * LG ThinQ Connect 드라이버(ACT-03.04, TC-ACT-074). 공개 API 문서의 모양으로 만든 에어컨 어댑터이고 키가 없어 기본 꺼짐이다(ADR-040).
 *
 * <ul>
 *   <li>명령: {@code POST /devices/{deviceId}/control} — Switch → {@code {"operation":{"airConOperationMode":"POWER_ON|POWER_OFF"}}},
 *       Thermostat → {@code {"airConJobMode":{"currentJobMode":"COOL"}, "temperature":{"targetTemperature":24}}}(mode off는 전원 끔).</li>
 *   <li>상태: {@code GET /devices/{deviceId}/state} → {@code response.operation·airConJobMode·temperature}.</li>
 *   <li>헤더: {@code x-country}, {@code x-client-id}(설정 clientId), {@code x-message-id}(명령 ID 22자 base64url), {@code x-api-key}(설정 apiKey).</li>
 *   <li>인증: 접근 토큰(비밀값 {@code token}). 401이면 비밀값 {@code refreshToken}으로 {@code POST /oauth/token}에서 갱신 뒤 1회만 다시.</li>
 *   <li>푸시(MQTT 통지 {@code {deviceId, report:{…}}})는 {@link #onPush}로 표준 상태 보고가 된다.</li>
 * </ul>
 */
public class LgThinqDriver extends CloudVendorDriver {

    public static final String TYPE = "LG_THINQ";
    private static final Map<String, String> MODES = Map.of("cool", "COOL", "heat", "HEAT", "dry", "AIR_DRY", "fan", "FAN", "auto", "AUTO");

    public LgThinqDriver(String baseUrl, Duration timeout, DriverEventSink sink, Clock clock) {
        super(baseUrl, timeout, sink, clock);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of("Switch", "Thermostat");
    }

    /** 명령 ID → x-message-id(16바이트 → base64url 22자) */
    public static String messageId(UUID id) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    @Override
    protected Map<String, String> headers(DriverConfig config, UUID commandId) {
        Map<String, String> h = new LinkedHashMap<>();
        h.put("x-country", config.string("country", "KR"));
        h.put("x-client-id", config.string("clientId", "data2flow"));
        h.put("x-message-id", messageId(commandId == null ? UUID.nameUUIDFromBytes("state".getBytes()) : commandId));
        String apiKey = config.string("apiKey", null);
        if (apiKey != null) {
            h.put("x-api-key", apiKey);
        }
        return h;
    }

    @Override
    protected String refreshPath() {
        return "/oauth/token";
    }

    @Override
    protected VendorRequest controlRequest(DriverCommand c) {
        Map<String, Object> body = new LinkedHashMap<>();
        Map<String, Object> a = c.args();
        switch (c.capability()) {
            case "Switch" -> body.put("operation", Map.of("airConOperationMode", Boolean.TRUE.equals(a.get("on")) ? "POWER_ON" : "POWER_OFF"));
            case "Thermostat" -> {
                Object mode = a.get("mode");
                if ("off".equals(mode)) {
                    body.put("operation", Map.of("airConOperationMode", "POWER_OFF"));
                } else if (mode != null) {
                    String vendorMode = MODES.get(mode.toString());
                    if (vendorMode == null) {
                        throw new IllegalArgumentException("ThinQ가 지원하지 않는 모드입니다: " + mode);
                    }
                    body.put("airConJobMode", Map.of("currentJobMode", vendorMode));
                }
                if (a.get("targetTemperature") != null) {
                    body.put("temperature", Map.of("targetTemperature", a.get("targetTemperature")));
                }
            }
            default -> throw new IllegalArgumentException("ThinQ 드라이버가 지원하지 않는 기능입니다: " + c.capability());
        }
        if (body.isEmpty()) {
            throw new IllegalArgumentException("보낼 값이 없습니다");
        }
        return new VendorRequest("POST", "/devices/" + c.device().externalId() + "/control", body);
    }

    @Override
    protected String statePath(String vendorDeviceId) {
        return "/devices/" + vendorDeviceId + "/state";
    }

    @Override
    protected String healthPath(DriverConfig config) {
        return "/devices";
    }

    @Override
    protected Map<String, Map<String, Object>> toStandardState(JsonNode body) {
        JsonNode r = body.has("response") ? body.path("response") : body;
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        JsonNode op = r.path("operation").path("airConOperationMode");
        boolean on = "POWER_ON".equals(op.asString(""));
        if (!op.isMissingNode()) {
            out.put("Switch", Map.of("on", on));
        }
        Map<String, Object> thermostat = new LinkedHashMap<>();
        JsonNode job = r.path("airConJobMode").path("currentJobMode");
        if (!op.isMissingNode() && !on) {
            thermostat.put("mode", "off");
        } else if (!job.isMissingNode()) {
            String std = MODES.entrySet().stream().filter(e -> e.getValue().equals(job.asString())).map(Map.Entry::getKey).findFirst()
                    .orElse("auto");
            thermostat.put("mode", std);
        }
        JsonNode target = r.path("temperature").path("targetTemperature");
        if (target.isNumber()) {
            thermostat.put("targetTemperature", target.asDouble());
        }
        JsonNode current = r.path("temperature").path("currentTemperature");
        if (current.isNumber()) {
            thermostat.put("currentTemperature", current.asDouble());
        }
        if (!thermostat.isEmpty()) {
            out.put("Thermostat", thermostat);
        }
        return out;
    }

    /** 푸시 통지({@code {"deviceId":"…","report":{…상태 응답과 같은 모양}}})를 표준 상태 보고로 넘긴다 */
    public boolean onPush(long organizationId, long deviceId, JsonNode push) {
        Map<String, Map<String, Object>> state = toStandardState(push.path("report"));
        report(organizationId, deviceId, state);
        return !state.isEmpty();
    }
}
