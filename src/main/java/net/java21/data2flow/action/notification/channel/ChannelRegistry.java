package net.java21.data2flow.action.notification.channel;

import net.java21.data2flow.contracts.notification.NotificationChannel;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 등록된 알림 채널 SPI 구현(OPS-06.06, BR-OPS-32). 채널은 빈으로 등록하고 공통 계층은 키로 찾기만 한다(채널 종류로 분기하지 않음).
 * 계약 키트({@code AbstractNotificationChannelContractTest})를 통과한 구현만 빈으로 둔다. 지금은 텔레그램 하나(ADR-033).
 */
public class ChannelRegistry {

    private final Map<String, NotificationChannel> byKey;

    public ChannelRegistry(List<NotificationChannel> channels) {
        this.byKey = channels.stream().collect(Collectors.toUnmodifiableMap(NotificationChannel::key, Function.identity()));
    }

    /** 키(대소문자 무시: 콜백 경로는 소문자 {@code /hooks/messenger/telegram}) */
    public Optional<NotificationChannel> find(String key) {
        return key == null ? Optional.empty() : Optional.ofNullable(byKey.get(key.toUpperCase(Locale.ROOT)));
    }

    public Collection<NotificationChannel> all() {
        return byKey.values();
    }
}
