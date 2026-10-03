package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;

/**
 * 드라이버 응답의 표준 정규화 입구(BR-ACT-25). MQTT·LoRaWAN·벤더 드라이버는 ack 토픽·상태 토픽·벤더 이벤트를 받아 여기로 넘기고,
 * 가상 장비(시뮬레이터)는 같은 모양의 이벤트를 {@code data2flow.events}로 직접 발행한다. 둘 다 같은 처리 경로({@code CommandTracker})를 탄다.
 */
public interface DriverEventSink {

    /** EVT-ACT-06 {@code device.command.ack} */
    void ack(long organizationId, DeviceCommandAck ack);

    /** EVT-ACT-07 {@code device.state.reported} */
    void reported(long organizationId, DeviceStateReported state);
}
