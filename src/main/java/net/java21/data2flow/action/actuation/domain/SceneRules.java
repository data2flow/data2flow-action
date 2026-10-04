package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.command.SourceType;

import java.util.Collection;

/**
 * 장면 규칙(BR-ACT-16, BR-ACT-24, ACT-05.01): 항목 100개 이하, 실행 시 관계 대상을 펼쳐 기기별 명령, 일부 실패해도 나머지는 적용(PARTIAL).
 * 장면 명령의 우선순위는 장면을 실행한 출처(화면 = MANUAL, 예약 = SCHEDULE, 플로우 = AUTO, AI = AI)를 따른다.
 */
public final class SceneRules {

    public static final int MAX_ITEMS = 100;

    /** 장면 실행 상태 */
    public enum RunStatus { RUNNING, SUCCEEDED, PARTIAL, FAILED }

    private SceneRules() {
    }

    /**
     * 장면 명령 출처: 실행 출처의 칸을 그대로 두고 종류만 SCENE, 장면 실행 ID를 더한다. 실행 출처 종류는 {@link #origin}으로 되찾는다.
     */
    public static CommandSource sceneSource(CommandSource runOrigin, String sceneRunId) {
        return new CommandSource(SourceType.SCENE, runOrigin.userId(), runOrigin.flowId(), runOrigin.flowVersion(), runOrigin.nodeId(),
                runOrigin.triggerMessageId(), sceneRunId, runOrigin.scheduleId(), null, runOrigin.suggestionId(), runOrigin.approvedBy(),
                runOrigin.spaceId());
    }

    /** 장면 명령 출처에서 실행 출처 종류를 되찾는다(AI 승인 > 플로우 > 예약 > 사용자 > 시스템) */
    public static SourceType origin(CommandSource sceneSource) {
        if (sceneSource.suggestionId() != null && sceneSource.approvedBy() != null) {
            return SourceType.AI;
        }
        if (sceneSource.flowId() != null) {
            return SourceType.FLOW;
        }
        if (sceneSource.scheduleId() != null) {
            return SourceType.SCHEDULE;
        }
        if (sceneSource.userId() != null) {
            return SourceType.USER;
        }
        return SourceType.SYSTEM;
    }

    /** 명령 상태들로 장면 실행 상태(진행 중이면 RUNNING) */
    public static RunStatus status(Collection<CommandStatus> statuses) {
        if (statuses.isEmpty()) {
            return RunStatus.FAILED;
        }
        int ok = 0;
        int bad = 0;
        for (CommandStatus s : statuses) {
            switch (s) {
                case APPLIED, ACKED, SKIPPED -> ok++;
                case FAILED, TIMEOUT, REJECTED, BLOCKED, CANCELLED, SUPERSEDED -> bad++;
                default -> {
                    return RunStatus.RUNNING;
                }
            }
        }
        if (bad == 0) {
            return RunStatus.SUCCEEDED;
        }
        return ok == 0 ? RunStatus.FAILED : RunStatus.PARTIAL;
    }
}
