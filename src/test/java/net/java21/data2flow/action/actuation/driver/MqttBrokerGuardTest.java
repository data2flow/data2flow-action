package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.action.actuation.driver.mqtt.MqttBrokerGuard;
import net.java21.data2flow.action.actuation.driver.mqtt.MqttDriver;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.support.MutableClock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 공용 브로커 안전장치(CLAUDE.md §5, ⏸ ACT-03.02 결정 대기): MQTT 드라이버는 공용 플랫폼 브로커 iot-data.java21.net에 접속하지 않는다.
 */
class MqttBrokerGuardTest {

    @ParameterizedTest(name = "{0} → 거부")
    @ValueSource(strings = {"iot-data.java21.net", "IOT-DATA.java21.net", "iot-data.java21.net.", "ws.iot-data.java21.net"})
    @DisplayName("[ACT-03.02] 공용 브로커 주소는 설정 금지 목록이 비어 있어도 거부한다")
    void sharedBrokerRefused(String host) {
        assertThatThrownBy(() -> MqttBrokerGuard.requireAllowed(host, List.of())).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CLAUDE.md");
    }

    @Test
    @DisplayName("[ACT-03.02] 금지 목록에 더한 주소·빈 주소도 거부, 시험 브로커(localhost)는 허용")
    void configured() {
        assertThatThrownBy(() -> MqttBrokerGuard.requireAllowed("broker.example.org", List.of("broker.example.org")))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> MqttBrokerGuard.requireAllowed(" ", List.of())).isInstanceOf(IllegalStateException.class);
        assertThatCode(() -> MqttBrokerGuard.requireAllowed("localhost", null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("[ACT-03.02] 드라이버를 공용 브로커로 만들 수 없다(생성 시 거부, 접속 시도 없음)")
    void driverRefusesSharedBroker() {
        ActionProperties.Mqtt settings = new ActionProperties.Mqtt(true, ActionProperties.Mqtt.SHARED_PLATFORM_BROKER, 443, null, null,
                null, null, 1);
        assertThatThrownBy(() -> new MqttDriver(settings, "data2flow-action-test-1", new DriverContractTest.RecordingSink(),
                new MutableClock(MutableClock.T0))).isInstanceOf(IllegalStateException.class);
    }
}
