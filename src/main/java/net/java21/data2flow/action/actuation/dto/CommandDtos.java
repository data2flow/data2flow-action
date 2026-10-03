package net.java21.data2flow.action.actuation.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.repository.CommandEventRepository.TimelineEntry;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.SourceType;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** 명령 API 모양(API-ACT-01·02, ACT-api 부록 A). ID는 문자열, 시각은 ISO-8601 UTC */
public final class CommandDtos {

    private CommandDtos() {
    }

    /**
     * {@code POST /internal/action/commands} 본문(API-ACT-01 본문 + source + priority, ACT-api §5.2).
     *
     * @param deviceId        대상 기기
     * @param capability      기능
     * @param command         명령
     * @param args            인자
     * @param validitySeconds 유효 시간(60~3600). 없으면 조직 기본
     * @param waitFor         {@code wait}: none(기본)·ack·applied
     * @param source          출처. 없으면 USER(요청 사용자)
     * @param priority        우선순위. 무시하고 출처로 정한다(BR-ACT-24)
     * @param sourceSpaceId   출처 공간(장면·예약 실행이 샌드박스인지, BR-ACT-23). 사용자 명령은 비움
     * @param idempotencyKey  멱등 키. 없으면 헤더 {@code Idempotency-Key}
     */
    public record SubmitCommandRequest(String deviceId, String capability, String command, Map<String, Object> args,
                                       Integer validitySeconds, @JsonProperty("wait") String waitFor, SourceRequest source, CommandPriority priority,
                                       String sourceSpaceId, String idempotencyKey) {
    }

    /** 출처 요청(ID는 문자열 또는 숫자) */
    public record SourceRequest(SourceType type, String userId, String flowId, Integer flowVersion, String nodeId, String triggerMessageId,
                                String sceneRunId, String scheduleId, String bulkJobId, String suggestionId, String approvedBy) {

        public CommandSource toSource() {
            return new CommandSource(type, id(userId), flowId, flowVersion, nodeId, triggerMessageId, sceneRunId, id(scheduleId), bulkJobId,
                    suggestionId, id(approvedBy));
        }

        private static Long id(String s) {
            return s == null || s.isBlank() ? null : Long.valueOf(s);
        }
    }

    /**
     * 명령 응답(API-ACT-01 {@code Command}).
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record CommandResponse(String id, String status, String statusReason, String deviceId, String capability, String command,
                                  Map<String, Object> args, String priority, SourceView source, Instant requestedAt, Instant validUntil,
                                  Instant executeAfter, Instant expectedDeliveryAt, Instant sentAt, Instant ackedAt, Instant appliedAt,
                                  Instant finishedAt, List<TimelineView> timeline, String message) {

        public static CommandResponse of(Command c, List<TimelineEntry> timeline, String message) {
            return new CommandResponse(c.id().toString(), c.status().name(), c.statusReason(), Long.toString(c.deviceId()), c.capability(),
                    c.command(), c.args(), c.priority().name(), SourceView.of(c.source()), c.requestedAt(), c.validUntil(), c.executeAfter(),
                    null, c.sentAt(), c.ackedAt(), c.appliedAt(), c.finishedAt(),
                    timeline == null ? null : timeline.stream().map(t -> new TimelineView(t.status(), t.at(), t.reason())).toList(), message);
        }
    }

    /**
     * 출처(ACT-02.03). 이름(플로우 이름·사용자 이름)은 core가 이력을 넘길 때 붙인다(ACT-api §5.2 "표시 이름").
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SourceView(String type, String userId, String flowId, Integer flowVersion, String nodeId, String triggerMessageId,
                             String sceneRunId, String scheduleId, String bulkJobId, String suggestionId, String approvedBy) {

        public static SourceView of(CommandSource s) {
            if (s == null) {
                return null;
            }
            return new SourceView(s.type().name(), str(s.userId()), s.flowId(), s.flowVersion(), s.nodeId(), s.triggerMessageId(),
                    s.sceneRunId(), str(s.scheduleId()), s.bulkJobId(), s.suggestionId(), str(s.approvedBy()));
        }

        private static String str(Long v) {
            return v == null ? null : v.toString();
        }
    }

    /** 타임라인 한 줄 */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record TimelineView(String status, Instant at, String reason) {
    }

    /** 거부된 명령 응답 본문(4xx의 {@code response}): 거부도 기록되므로 ID를 돌려준다 */
    public record RejectedCommand(String commandId, String status, String statusReason) {
    }
}
