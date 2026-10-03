package net.java21.data2flow.action.actuation.driver;

import java.util.Set;

/**
 * 연결 확인 결과(API-ACT-31 {@code {ok, latencyMs, capabilities[], error?:{kind, message}}}).
 *
 * @param ok           성공
 * @param latencyMs    걸린 시간
 * @param capabilities 지원 기능
 * @param errorKind    실패 종류(UNREACHABLE·AUTH·REFUSED …)
 * @param message      실패 설명
 */
public record DriverHealth(boolean ok, long latencyMs, Set<String> capabilities, String errorKind, String message) {

    public static DriverHealth up(long latencyMs, Set<String> capabilities) {
        return new DriverHealth(true, latencyMs, capabilities, null, null);
    }

    public static DriverHealth down(long latencyMs, Set<String> capabilities, String kind, String message) {
        return new DriverHealth(false, latencyMs, capabilities, kind, message);
    }
}
