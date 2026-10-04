package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.notification.NotificationErrorCode;
import net.java21.data2flow.action.notification.channel.CallbackAcknowledger;
import net.java21.data2flow.action.notification.channel.ChannelRegistry;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.MessagePayload;
import net.java21.data2flow.action.notification.domain.NotificationTexts;
import net.java21.data2flow.action.notification.repository.DeliveryRepository;
import net.java21.data2flow.action.notification.service.NotificationCoreClient.LinkedUser;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.notification.CallbackAction;
import net.java21.data2flow.contracts.notification.CallbackCommand;
import net.java21.data2flow.contracts.notification.ChannelMessage;
import net.java21.data2flow.contracts.notification.ChannelSettings;
import net.java21.data2flow.contracts.notification.InboundRequest;
import net.java21.data2flow.contracts.notification.NotificationChannel;
import net.java21.data2flow.contracts.notification.SendResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 메신저 콜백(RUL-05.02, API-RUL-31, BR-RUL-18): 외부 → BFF {@code /hooks/messenger/{channel}} → 여기로 원본 그대로.
 *
 * <ol>
 *   <li>그 종류의 켜진 채널 설정 중 시크릿이 맞는 것을 찾는다(채널 SPI {@code verify}). 없으면 401.</li>
 *   <li>{@code handleCallback}으로 공통 명령으로 바꾼다(처리할 것이 없으면 아무것도 하지 않음).</li>
 *   <li>LINK: 일회용 코드로 계정 연결(API-RUL-47) 후 결과 안내.</li>
 *   <li>ACK·MUTE_30M: 연결된 계정(API-RUL-46)이 없으면 처리하지 않고 연결 안내만 보낸다. 있으면 그 사용자 권한으로 core에 확인·무음
 *       (API-RUL-48·49) → 성공하면 같은 메시지를 결과로 갱신(버튼 제거).</li>
 * </ol>
 */
public class MessengerCallbackService {

    private static final Logger log = LoggerFactory.getLogger(MessengerCallbackService.class);

    private final ChannelRegistry channels;
    private final NotificationCoreClient core;
    private final DeliveryRepository deliveries;

    public MessengerCallbackService(ChannelRegistry channels, NotificationCoreClient core, DeliveryRepository deliveries) {
        this.channels = channels;
        this.core = core;
        this.deliveries = deliveries;
    }

    /** 처리 결과 */
    public record Result(boolean handled, String action, String outcome) {
    }

    public Result handle(String channelKey, InboundRequest request) {
        NotificationChannel impl = channels.find(channelKey).orElseThrow(() -> new BusinessException(NotificationErrorCode.CHANNEL_TYPE_UNKNOWN));
        ChannelDefinition def = core.channels(null, impl.key()).stream()
                .filter(c -> impl.verify(c.settings(), request)).findFirst()
                .orElseThrow(() -> new BusinessException(NotificationErrorCode.CALLBACK_REJECTED));
        ChannelSettings settings = def.settings();
        Optional<CallbackCommand> cmd = impl.handleCallback(settings, request);
        if (cmd.isEmpty()) {
            return new Result(false, null, "IGNORED");
        }
        CallbackCommand c = cmd.get();
        String locale = "ko";
        if (c.action() == CallbackAction.LINK) {
            Optional<LinkedUser> linked = core.confirmLink(impl.key(), c.linkCode(), c.externalUserId());
            reply(impl, settings, c, c.externalUserId(), NotificationTexts.text(linked.isPresent() ? "linked" : "linkFailed", locale));
            return new Result(true, c.action().name(), linked.isPresent() ? "LINKED" : "LINK_FAILED");
        }
        if (c.action() != CallbackAction.ACK && c.action() != CallbackAction.MUTE_30M || c.alarmId() == null) {
            reply(impl, settings, c, c.externalUserId(), NotificationTexts.text("unsupported", locale));
            return new Result(true, c.action().name(), "UNSUPPORTED");
        }
        Optional<LinkedUser> user = core.linkedUser(impl.key(), c.externalUserId());
        Optional<Delivery> delivery = deliveryOf(c);
        String address = delivery.map(Delivery::payload).map(MessagePayload::address).orElse(c.externalUserId());
        if (user.isEmpty()) {
            // BR-RUL-18: 연결되지 않은 계정은 처리하지 않고 연결 방법만 안내한다(TC-RUL-094)
            reply(impl, settings, c, c.externalUserId(), NotificationTexts.text("notLinked", locale));
            return new Result(true, c.action().name(), "NOT_LINKED");
        }
        boolean ack = c.action() == CallbackAction.ACK;
        int status = core.alarmAction(c.alarmId(), ack ? "ack" : "mute", user.get(), ack ? null : 30);
        if (status < 200 || status >= 300) {
            acknowledge(impl, settings, c, "HTTP " + status);
            return new Result(true, c.action().name(), status == 403 ? "FORBIDDEN" : status == 404 ? "NOT_FOUND" : "FAILED");
        }
        String resultText = NotificationTexts.text(ack ? "acked" : "muted", delivery.map(d -> d.payload() == null ? null : d.payload().locale())
                .orElse(locale));
        acknowledge(impl, settings, c, resultText);
        // 같은 메시지를 결과로 갱신(버튼 제거, BR-RUL-18)
        if (delivery.isPresent() && delivery.get().payload() != null && delivery.get().organizationId() == user.get().organizationId()) {
            Delivery d = delivery.get();
            String replaceId = c.externalMessageId() != null ? c.externalMessageId() : d.externalMessageId();
            MessagePayload updated = d.payload().withBody(d.payload().body() + "\n" + resultText, List.of(), replaceId);
            deliveries.updatePayload(d.id(), updated);
            if (replaceId != null) {
                SendResult r = impl.send(settings, updated.toMessage(ActionIdempotencyKeys.of("edit", d.id().toString(), c.action().name()))
                        .adaptTo(impl.capabilities()));
                if (r.outcome() != SendResult.Outcome.SUCCESS) {
                    log.info("메시지 갱신 실패 delivery={}: {}", d.id(), r.error());
                }
            }
        } else {
            reply(impl, settings, c, address, resultText);
        }
        return new Result(true, c.action().name(), ack ? "ACKED" : "MUTED");
    }

    private Optional<Delivery> deliveryOf(CallbackCommand c) {
        if (c.deliveryId() == null) {
            return Optional.empty();
        }
        try {
            return deliveries.findForCallback(UUID.fromString(c.deliveryId()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private void reply(NotificationChannel impl, ChannelSettings settings, CallbackCommand c, String address, String text) {
        acknowledge(impl, settings, c, text);
        SendResult r = impl.send(settings, new ChannelMessage(ActionIdempotencyKeys.of("reply", c.externalUserId(), text,
                String.valueOf(c.callbackId())), address, null, text, null, null, List.of(), "ko", null).adaptTo(impl.capabilities()));
        if (r.outcome() != SendResult.Outcome.SUCCESS) {
            log.info("메신저 안내 발송 실패: {}", r.error());
        }
    }

    private static void acknowledge(NotificationChannel impl, ChannelSettings settings, CallbackCommand c, String text) {
        if (impl instanceof CallbackAcknowledger a) {
            a.acknowledge(settings, c, text);
        }
    }
}
