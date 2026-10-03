package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * 명령 한 건({@code data2flow_action.commands}, ACT domain-model §2·§3).
 *
 * @param id             명령 ID(UUID)
 * @param organizationId 조직
 * @param idempotencyKey 저장한 멱등 키(64자)
 * @param deviceId       기기
 * @param capability     기능
 * @param command        명령(set)
 * @param args           인자
 * @param priority       우선순위(출처가 정함, BR-ACT-24)
 * @param source         출처(BR-ACT-15)
 * @param status         상태
 * @param statusReason   사유(CommandStatusReasons)
 * @param validUntil     유효 시각
 * @param executeAfter   보호 지연 실행 시각(DELAYED)
 * @param attempts       드라이버 호출 횟수
 * @param requestedAt    요청 시각
 * @param sentAt         드라이버 전달 시각
 * @param ackedAt        ack 시각
 * @param appliedAt      적용 확인 시각
 * @param finishedAt     끝난 시각
 * @param timeoutAt      지금 상태의 기한
 */
public record Command(UUID id, long organizationId, String idempotencyKey, long deviceId, String capability, String command,
                      Map<String, Object> args, CommandPriority priority, CommandSource source, CommandStatus status,
                      String statusReason, Instant validUntil, Instant executeAfter, int attempts, Instant requestedAt,
                      Instant sentAt, Instant ackedAt, Instant appliedAt, Instant finishedAt, Instant timeoutAt) {

    public Command {
        args = args == null ? Map.of() : Map.copyOf(args);
    }
}
