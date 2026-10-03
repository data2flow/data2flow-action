package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;

import java.time.Instant;
import java.util.Map;

/**
 * 제어 창구 입력(화면·플로우·AI·예약 어디서 오든 한 모양, ADR-009).
 *
 * @param organizationId    조직
 * @param deviceId          대상 기기
 * @param capability        기능
 * @param command           명령
 * @param args              인자
 * @param source            출처(BR-ACT-15)
 * @param requestedPriority 요청에 들어 있던 우선순위(무시하고 감사에만 남긴다, BR-ACT-24). 없으면 null
 * @param idempotencyKey    저장할 멱등 키(64자, {@code IdempotencyKeys})
 * @param validUntil        유효 시각. 없으면 {@code validitySeconds} 또는 조직 기본
 * @param validitySeconds   유효 시간(60~3600, API-ACT-01). 없으면 null
 * @param sourceSpaceId     출처 공간(샌드박스 판정 BR-ACT-23). 없으면 null
 * @param checkPermission   요청 사용자 권한을 검사할지(내부 API의 사용자 명령)
 * @param expired           이미 유효 시각이 지난 행동 요청(FAILED(EXPIRED)로 기록만)
 */
public record CommandRequest(long organizationId, long deviceId, String capability, String command, Map<String, Object> args,
                             CommandSource source, CommandPriority requestedPriority, String idempotencyKey, Instant validUntil,
                             Integer validitySeconds, Long sourceSpaceId, boolean checkPermission, boolean expired) {

    public CommandRequest {
        args = args == null ? Map.of() : Map.copyOf(args);
    }
}
