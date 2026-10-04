package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.lorawan.DownlinkEncoder;
import net.java21.data2flow.action.common.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.HexFormat;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** LoRaWAN 다운링크 인코딩(ACT-03.03) */
class DownlinkEncoderTest {

    @ParameterizedTest(name = "골든 픽스처 {0}")
    @ValueSource(strings = {"switch-on", "switch-off", "thermostat-cool-24"})
    @DisplayName("[ACT-03.03][TC-ACT-073] 모델 인코딩: Switch on/off, Thermostat set(cool,24) → fPort·payload가 골든 픽스처와 같다")
    void golden(String name) throws IOException {
        JsonNode f;
        try (InputStream in = getClass().getResourceAsStream("/fixtures/downlink/" + name + ".json")) {
            f = Json.MAPPER.readTree(in);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> args = Json.MAPPER.convertValue(f.path("args"), Map.class);
        DownlinkEncoder.Downlink d = DownlinkEncoder.encode(f.path("capability").asString(), "set", args, null);
        assertThat(d.fPort()).isEqualTo(f.path("fPort").asInt());
        assertThat(HexFormat.of().formatHex(d.bytes())).isEqualTo(f.path("payloadHex").asString());
    }

    @Test
    @DisplayName("[ACT-03.03][TC-ACT-073] 지원하지 않는 기능·명령·값 → CAPABILITY_NOT_SUPPORTED, fPortDefault가 있으면 그 포트")
    void unsupported() {
        assertThatThrownBy(() -> DownlinkEncoder.encode("Contact", "set", Map.of(), null)).hasMessageContaining("CAPABILITY_NOT_SUPPORTED");
        assertThatThrownBy(() -> DownlinkEncoder.encode("Switch", "toggle", Map.of(), null)).hasMessageContaining("CAPABILITY_NOT_SUPPORTED");
        assertThatThrownBy(() -> DownlinkEncoder.encode("Dimmer", "set", Map.of("level", 300), null)).hasMessageContaining("범위");
        assertThatThrownBy(() -> DownlinkEncoder.encode("Thermostat", "set", Map.of("mode", "turbo"), null)).hasMessageContaining("모르는");
        assertThatThrownBy(() -> DownlinkEncoder.encode("Switch", "set", Map.of("on", "yes"), null)).hasMessageContaining("boolean");
        assertThatThrownBy(() -> DownlinkEncoder.encode("Thermostat", "set", Map.of("targetTemperature", 200), null)).hasMessageContaining("온도");
        assertThat(DownlinkEncoder.encode("Switch", "set", Map.of("on", true), 2).fPort()).isEqualTo(2);
        assertThat(HexFormat.of().formatHex(DownlinkEncoder.encode("FanSpeed", "set", Map.of("level", 3), null).bytes())).isEqualTo("0403ff");
        assertThat(HexFormat.of().formatHex(DownlinkEncoder.encode("Ventilation", "set", Map.of("mode", "auto"), null).bytes())).isEqualTo("0502ff");
        assertThat(HexFormat.of().formatHex(DownlinkEncoder.encode("Lock", "set", Map.of("locked", true), null).bytes())).isEqualTo("0601");
        assertThat(HexFormat.of().formatHex(DownlinkEncoder.encode("Dimmer", "set", Map.of("level", 50), null).bytes())).isEqualTo("0332");
    }
}
