package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.SourceType;

import java.util.regex.Pattern;

/**
 * 명령 멱등 키 정규화({@code commands.idempotency_key char(64)}, UQ(조직, 키)). 사용자 {@code Idempotency-Key}는 사용자마다 따로
 * 보도록 {@code sha256("user", userId, key)}로 바꾸고, 플로우 키(SHA-256 16진수 64자, BR-FLW-13)는 그대로 쓴다.
 */
public final class IdempotencyKeys {

    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    private IdempotencyKeys() {
    }

    /** 내부 API 사용자 명령의 저장 키 */
    public static String user(long userId, String headerKey) {
        return ActionIdempotencyKeys.of("user", Long.toString(userId), headerKey);
    }

    /** 행동 요청(큐)의 저장 키. 이미 SHA-256이면 그대로 */
    public static String action(SourceType type, String key) {
        return SHA256_HEX.matcher(key).matches() ? key : ActionIdempotencyKeys.of("action", type.name(), key);
    }

    /** 관계 대상을 기기별로 펼친 명령의 키 */
    public static String expanded(String requestKey, long deviceId) {
        return ActionIdempotencyKeys.of("target", requestKey, Long.toString(deviceId));
    }
}
