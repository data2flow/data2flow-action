package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.contracts.error.ErrorCode;
import net.java21.data2flow.contracts.error.FieldErrorDetail;

import java.time.Duration;
import java.util.List;

/**
 * 제어 창구 결과.
 *
 * @param command   명령(거부된 명령도 기록된다)
 * @param replay    같은 멱등 키의 이전 결과
 * @param rejection 거부·차단이면 API 오류 정보, 아니면 null
 * @param message   차단 사유 문구(인터락 message 등). 없으면 null
 */
public record Outcome(Command command, boolean replay, Rejection rejection, String message) {

    /**
     * @param code       오류 코드
     * @param errors     입력 검증 위반
     * @param args       문구 자리표시자(최소·최대, 남은 초)
     * @param retryAfter 429 Retry-After. 없으면 null
     */
    public record Rejection(ErrorCode code, List<FieldErrorDetail> errors, Object[] args, Duration retryAfter) {
    }
}
