package net.java21.data2flow.action.actuation.driver.mqtt;

import net.java21.data2flow.action.common.ActionProperties;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 공용 브로커 접속 금지(CLAUDE.md §5, ⏸ ACT-03.02 결정 대기). 공용 플랫폼 브로커 {@code iot-data.java21.net}은 인증을 통과하면 발행도
 * 할 수 있으므로, 장비 명령을 내는 MQTT 드라이버가 그 주소로 접속하지 못하게 막는다. 금지 목록은 설정으로 늘릴 수만 있고
 * 공용 브로커는 항상 들어 있다. 이름이 같거나 그 하위 이름이면 막고, 이름을 풀 수 있으면 같은 IP로 우회하는 것도 막는다.
 */
public final class MqttBrokerGuard {

    private MqttBrokerGuard() {
    }

    /** 금지 주소면 {@link IllegalStateException} */
    public static void requireAllowed(String host, List<String> configuredDenied) {
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("MQTT 드라이버 브로커 주소(data2flow.action.mqtt.host)가 없습니다");
        }
        String h = normalize(host);
        for (String denied : denied(configuredDenied)) {
            if (h.equals(denied) || h.endsWith("." + denied)) {
                throw new IllegalStateException("공용 브로커에는 명령을 발행할 수 없습니다(CLAUDE.md §5): " + host);
            }
        }
        Set<String> deniedAddresses = new LinkedHashSet<>();
        for (String denied : denied(configuredDenied)) {
            deniedAddresses.addAll(resolve(denied));
        }
        for (String address : resolve(h)) {
            if (deniedAddresses.contains(address)) {
                throw new IllegalStateException("공용 브로커와 같은 주소에는 명령을 발행할 수 없습니다(CLAUDE.md §5): " + host);
            }
        }
    }

    static Set<String> denied(List<String> configured) {
        Set<String> out = new LinkedHashSet<>();
        out.add(ActionProperties.Mqtt.SHARED_PLATFORM_BROKER);
        if (configured != null) {
            configured.stream().filter(s -> s != null && !s.isBlank()).map(MqttBrokerGuard::normalize).forEach(out::add);
        }
        return out;
    }

    private static String normalize(String host) {
        String h = host.trim().toLowerCase(Locale.ROOT);
        return h.endsWith(".") ? h.substring(0, h.length() - 1) : h;
    }

    private static Set<String> resolve(String host) {
        try {
            Set<String> out = new LinkedHashSet<>();
            Arrays.stream(InetAddress.getAllByName(host)).map(InetAddress::getHostAddress).forEach(out::add);
            return out;
        } catch (Exception e) {
            return Set.of();
        }
    }
}
