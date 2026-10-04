package net.java21.data2flow.action.actuation.driver.vendor;

import net.java21.data2flow.action.actuation.driver.DriverCommand;
import net.java21.data2flow.action.actuation.driver.DriverConfig;
import net.java21.data2flow.action.actuation.driver.DriverEventSink;
import tools.jackson.databind.JsonNode;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * SmartThings 드라이버(ACT-03.04, TC-ACT-075). 공개 API 문서의 모양으로 만든 어댑터이고 키가 없어 기본 꺼짐이다(ADR-040).
 *
 * <ul>
 *   <li>명령: {@code POST /v1/devices/{deviceId}/commands {"commands":[{"component":"main","capability":"switch","command":"on"}]}}.
 *       Switch → switch on/off, Thermostat → thermostatMode.setThermostatMode·thermostatCoolingSetpoint.setCoolingSetpoint,
 *       Dimmer → switchLevel.setLevel, Lock → lock.lock/unlock.</li>
 *   <li>상태: {@code GET /v1/devices/{deviceId}/status} → {@code components.main.<capability>.<attribute>.value}.</li>
 *   <li>구독 이벤트(웹훅 {@code lifecycle=EVENT}의 {@code deviceEvent})는 {@link #onEvent}로 표준 상태 보고가 된다.</li>
 *   <li>인증: 개인 토큰(Bearer, 비밀값 {@code token}). 429는 일시 실패(재시도 백오프).</li>
 * </ul>
 */
public class SmartThingsDriver extends CloudVendorDriver {

    public static final String TYPE = "SMARTTHINGS";

    public SmartThingsDriver(String baseUrl, Duration timeout, DriverEventSink sink, Clock clock) {
        super(baseUrl, timeout, sink, clock);
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of("Switch", "Thermostat", "Dimmer", "Lock");
    }

    @Override
    protected Map<String, String> headers(DriverConfig config, UUID commandId) {
        return commandId == null ? Map.of() : Map.of("X-Request-Id", commandId.toString());
    }

    @Override
    protected VendorRequest controlRequest(DriverCommand c) {
        List<Map<String, Object>> commands = new ArrayList<>();
        Map<String, Object> a = c.args();
        switch (c.capability()) {
            case "Switch" -> commands.add(cmd("switch", Boolean.TRUE.equals(a.get("on")) ? "on" : "off", List.of()));
            case "Thermostat" -> {
                if (a.get("mode") != null) {
                    commands.add(cmd("thermostatMode", "setThermostatMode", List.of(a.get("mode").toString())));
                }
                if (a.get("targetTemperature") != null) {
                    commands.add(cmd("thermostatCoolingSetpoint", "setCoolingSetpoint", List.of(a.get("targetTemperature"))));
                }
            }
            case "Dimmer" -> commands.add(cmd("switchLevel", "setLevel", List.of(a.get("level"))));
            case "Lock" -> commands.add(cmd("lock", Boolean.TRUE.equals(a.get("locked")) ? "lock" : "unlock", List.of()));
            default -> throw new IllegalArgumentException("SmartThings가 지원하지 않는 기능입니다: " + c.capability());
        }
        if (commands.isEmpty()) {
            throw new IllegalArgumentException("보낼 값이 없습니다");
        }
        return new VendorRequest("POST", "/v1/devices/" + c.device().externalId() + "/commands", Map.of("commands", commands));
    }

    private static Map<String, Object> cmd(String capability, String command, List<Object> arguments) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("component", "main");
        m.put("capability", capability);
        m.put("command", command);
        m.put("arguments", arguments);
        return m;
    }

    @Override
    protected String statePath(String vendorDeviceId) {
        return "/v1/devices/" + vendorDeviceId + "/status";
    }

    @Override
    protected String healthPath(DriverConfig config) {
        return "/v1/locations";
    }

    @Override
    protected Map<String, Map<String, Object>> toStandardState(JsonNode body) {
        JsonNode main = body.path("components").path("main");
        Map<String, Map<String, Object>> out = new LinkedHashMap<>();
        JsonNode sw = main.path("switch").path("switch").path("value");
        if (!sw.isMissingNode()) {
            out.put("Switch", Map.of("on", "on".equals(sw.asString())));
        }
        Map<String, Object> thermostat = new LinkedHashMap<>();
        JsonNode mode = main.path("thermostatMode").path("thermostatMode").path("value");
        if (!mode.isMissingNode()) {
            thermostat.put("mode", mode.asString());
        }
        JsonNode setpoint = main.path("thermostatCoolingSetpoint").path("coolingSetpoint").path("value");
        if (setpoint.isNumber()) {
            thermostat.put("targetTemperature", setpoint.asDouble());
        }
        JsonNode current = main.path("temperatureMeasurement").path("temperature").path("value");
        if (current.isNumber()) {
            thermostat.put("currentTemperature", current.asDouble());
        }
        if (!thermostat.isEmpty()) {
            out.put("Thermostat", thermostat);
        }
        JsonNode level = main.path("switchLevel").path("level").path("value");
        if (level.isNumber()) {
            out.put("Dimmer", Map.of("level", level.asInt()));
        }
        JsonNode lock = main.path("lock").path("lock").path("value");
        if (!lock.isMissingNode()) {
            out.put("Lock", Map.of("locked", "locked".equals(lock.asString())));
        }
        return out;
    }

    /**
     * 구독 이벤트(웹훅 {@code {"lifecycle":"EVENT","eventData":{"events":[{"eventType":"DEVICE_EVENT","deviceEvent":{…}}]}}})를 표준 상태
     * 보고로 넘긴다. 반환: 넘긴 이벤트 수.
     */
    public int onEvent(long organizationId, long deviceId, JsonNode webhook) {
        int n = 0;
        for (JsonNode e : webhook.path("eventData").path("events")) {
            JsonNode d = e.path("deviceEvent");
            if (!"DEVICE_EVENT".equals(e.path("eventType").asString()) || d.isMissingNode()) {
                continue;
            }
            Map<String, Object> attr = new LinkedHashMap<>();
            Map<String, Object> component = new LinkedHashMap<>();
            component.put(d.path("attribute").asString(), Map.of("value", d.path("value").isNumber() ? d.path("value").asDouble()
                    : d.path("value").asString()));
            attr.put(d.path("capability").asString(), component);
            Map<String, Map<String, Object>> state = toStandardState(net.java21.data2flow.action.common.Json.MAPPER.valueToTree(
                    Map.of("components", Map.of("main", attr))));
            report(organizationId, deviceId, state);
            n++;
        }
        return n;
    }
}
