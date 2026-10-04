package net.java21.data2flow.action.actuation.event;

import com.rabbitmq.client.Channel;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.actuation.service.CommandTracker;
import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.contracts.message.DomainEvent;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.message.event.DeviceChanged;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import net.java21.data2flow.contracts.message.event.EventPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;

import java.io.IOException;
import java.util.List;

/**
 * {@code action.events} 소비(architecture.md §4.5). 이 서비스가 묶는 라우팅 키:
 * {@code device.command.ack}(EVT-ACT-06), {@code device.state.reported}(EVT-ACT-07: 시뮬레이터 상태 보고, pipeline LoRaWAN 업링크 신호 =
 * 빈 {@code capabilities} → Class A 대기 다운링크 전송), {@code device.connectivity.changed}(EVT-DEV-02),
 * {@code device.changed}(EVT-DEV-01, 삭제 시 정리·캐시 무효화), {@code control.emergency.started|released}(EVT-ACT-03: 대기 중 자동 명령 취소).
 * 처리는 멱등이고(상태 전이·버전 비교) 커밋 뒤 ACK한다.
 */
public class DeviceEventListener implements ChannelAwareMessageListener {

    /** 바인딩 라우팅 키 */
    public static final List<String> ROUTING_KEYS = List.of("device.command.ack", "device.state.reported",
            "device.connectivity.changed", "device.changed", "control.emergency.started", "control.emergency.released");
    private static final Logger log = LoggerFactory.getLogger(DeviceEventListener.class);

    private final CommandTracker tracker;
    private final ShadowRepository shadows;
    private final ControlProfileService profiles;
    private final MessageCodec codec = MessageCodec.create();
    private final net.java21.data2flow.action.actuation.service.EmergencyStopHandler emergency;

    public DeviceEventListener(CommandTracker tracker, ShadowRepository shadows, ControlProfileService profiles,
                               net.java21.data2flow.action.actuation.service.EmergencyStopHandler emergency) {
        this.tracker = tracker;
        this.shadows = shadows;
        this.profiles = profiles;
        this.emergency = emergency;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long tag = message.getMessageProperties().getDeliveryTag();
        DomainEvent<? extends EventPayload> event;
        try {
            event = codec.readEvent(message.getBody());
        } catch (MessageFormatException e) {
            log.warn("이벤트를 읽을 수 없어 DLQ로 보냅니다: {}", e.getMessage());
            channel.basicReject(tag, false);
            return;
        }
        try {
            handle(event);
            channel.basicAck(tag, false);
        } catch (RuntimeException e) {
            log.warn("이벤트 처리 실패(다시 받음) type={} messageId={}: {}", event.type(), event.messageId(), e.toString());
            channel.basicNack(tag, false, true);
        }
    }

    /** 이벤트 하나 처리(시험에서 직접 부른다) */
    public void handle(DomainEvent<? extends EventPayload> event) {
        long org = event.organizationId();
        switch (event.payload()) {
            case DeviceCommandAck ack -> tracker.ack(org, ack);
            case DeviceStateReported state -> tracker.reported(org, state);
            case DeviceConnectivityChanged c -> tracker.connectivity(org, c);
            case DeviceChanged d -> {
                profiles.invalidate(d.deviceId());
                if (d.change() == DeviceChanged.Change.DELETED) {
                    shadows.deleteDevice(org, d.deviceId());
                }
            }
            case net.java21.data2flow.contracts.message.event.EmergencyStopChanged stop -> {
                if (event.type().equals(net.java21.data2flow.contracts.message.EventType.CONTROL_EMERGENCY_STARTED.routingKey())) {
                    emergency.started(org, stop);
                } else {
                    emergency.released(org, stop);
                }
            }
            default -> {
                // 묶지 않은 종류: 무시
            }
        }
    }
}
