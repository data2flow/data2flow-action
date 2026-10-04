package net.java21.data2flow.action.actuation.driver.lorawan;

import net.java21.data2flow.action.common.ActionProperties;

import java.net.InetAddress;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 공용 ChirpStack 다운링크 금지(CLAUDE.md §5, ⏸ ACT-03.03 결정 대기). 아카데미 공용 ChirpStack(s3.java21.net)과 공용 MQTT 브로커
 * (iot-data.java21.net)는 운영 중인 공용 인프라라 다운링크 등록(쓰기)을 하지 않는다. 금지 목록은 설정으로 늘릴 수만 있고 공용 주소는 항상
 * 들어 있다. 이름이 같거나 하위 이름이면 막고, 이름을 풀 수 있으면 같은 IP로 우회하는 것도 막는다.
 */
public final class ChirpStackHostGuard {

    private ChirpStackHostGuard() {
    }

    /** 금지 주소면 {@link IllegalStateException} */
    public static void requireAllowed(String chirpstackUrl, List<String> configuredDenied) {
        if (chirpstackUrl == null || chirpstackUrl.isBlank()) {
            throw new IllegalStateException("LoRaWAN 드라이버 설정에 chirpstackUrl이 없습니다");
        }
        String host;
        try {
            host = URI.create(chirpstackUrl.trim()).getHost();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("chirpstackUrl 형식이 올바르지 않습니다: " + chirpstackUrl);
        }
        if (host == null || host.isBlank()) {
            throw new IllegalStateException("chirpstackUrl에 호스트가 없습니다: " + chirpstackUrl);
        }
        String h = normalize(host);
        Set<String> denied = denied(configuredDenied);
        for (String d : denied) {
            if (h.equals(d) || h.endsWith("." + d)) {
                throw new IllegalStateException("공용 ChirpStack에는 다운링크를 넣을 수 없습니다(CLAUDE.md §5): " + host);
            }
        }
        Set<String> deniedAddresses = new LinkedHashSet<>();
        denied.forEach(d -> deniedAddresses.addAll(resolve(d)));
        for (String address : resolve(h)) {
            if (deniedAddresses.contains(address)) {
                throw new IllegalStateException("공용 ChirpStack과 같은 주소에는 다운링크를 넣을 수 없습니다(CLAUDE.md §5): " + host);
            }
        }
    }

    static Set<String> denied(List<String> configured) {
        Set<String> out = new LinkedHashSet<>(ActionProperties.LoRaWan.SHARED_HOSTS);
        if (configured != null) {
            configured.stream().filter(s -> s != null && !s.isBlank()).map(ChirpStackHostGuard::normalize).forEach(out::add);
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
