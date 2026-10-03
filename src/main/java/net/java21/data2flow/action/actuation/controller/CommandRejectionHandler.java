package net.java21.data2flow.action.actuation.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import net.java21.data2flow.action.actuation.dto.CommandDtos.RejectedCommand;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import net.java21.data2flow.contracts.web.ApiHeader;
import net.java21.data2flow.contracts.web.ErrorMessages;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

/**
 * 거부된 명령 응답: 공통 실패 모양({@code header}, {@code errors})에 {@code response:{commandId, status, statusReason}}을 더한다
 * (API-ACT-01 "거부된 명령도 항상 기록합니다… 4xx 응답의 response.commandId"). 429는 {@code Retry-After}를 붙인다.
 */
@RestControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CommandRejectionHandler {

    private final ErrorMessages messages;

    public CommandRejectionHandler(ErrorMessages messages) {
        this.messages = messages;
    }

    @ExceptionHandler(CommandRejectedException.class)
    public ResponseEntity<RejectedResponse> rejected(CommandRejectedException ex) {
        var r = ex.rejection();
        var builder = ResponseEntity.status(r.code().httpStatus());
        if (r.retryAfter() != null) {
            builder.header(DataflowHeaders.RETRY_AFTER, Long.toString(Math.max(1, r.retryAfter().toSeconds())));
        }
        return builder.body(new RejectedResponse(ApiHeader.failure(r.code().code(), messages.resolve(r.code(), r.args())),
                new RejectedCommand(ex.command().id().toString(), ex.command().status().name(), ex.command().statusReason()),
                r.errors()));
    }

    /** 실패 응답 + 기록된 명령 */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public record RejectedResponse(ApiHeader header, RejectedCommand response, List<FieldErrorDetail> errors) {
    }
}
