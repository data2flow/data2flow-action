package net.java21.data2flow.action.output.domain;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 출력 연결 접속 금지 주소(CLAUDE.md §5). 공용 플랫폼 브로커 {@code iot-data.java21.net}은 인증을 통과하면 발행도 되므로 출력 연결이
 * 그 주소(하위 이름·같은 IP 포함)로 접속하지 않게 막는다. 설정으로 금지 주소를 늘릴 수만 있다(actuation MqttBrokerGuard와 같은 규칙).
 */
public final class HostGuard {

    public static final String SHARED_PLATFORM_BROKER = "iot-data.java21.net";

    private HostGuard() {
    }

    /** URL의 호스트가 금지 주소인가(빈 주소·형식 오류도 금지) */
    public static boolean denied(String url, List<String> configured) {
        String host;
        try {
            host = URI.create(url.strip()).getHost();
        } catch (RuntimeException e) {
            return true;
        }
        return host == null || deniedHost(host, configured);
    }

    public static boolean deniedHost(String host, List<String> configured) {
        String h = normalize(host);
        Set<String> denied = denied(configured);
        for (String d : denied) {
            if (h.equals(d) || h.endsWith("." + d)) {
                return true;
            }
        }
        Set<String> deniedAddresses = new LinkedHashSet<>();
        denied.forEach(d -> deniedAddresses.addAll(resolve(d)));
        return resolve(h).stream().anyMatch(deniedAddresses::contains);
    }

    static Set<String> denied(List<String> configured) {
        Set<String> out = new LinkedHashSet<>();
        out.add(SHARED_PLATFORM_BROKER);
        if (configured != null) {
            configured.stream().filter(s -> s != null && !s.isBlank()).map(HostGuard::normalize).forEach(out::add);
        }
        return out;
    }

    private static String normalize(String host) {
        String h = host.strip().toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) {
            h = h.substring(1, h.length() - 1);
        }
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
