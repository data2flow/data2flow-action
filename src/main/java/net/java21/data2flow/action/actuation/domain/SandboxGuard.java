package net.java21.data2flow.action.actuation.domain;

import net.java21.data2flow.contracts.command.SourceType;

import java.util.Set;

/**
 * 샌드박스(SIM-07.03, BR-ACT-23): 샌드박스 공간을 출처로 하는 자동 명령(플로우·규칙·장면·예약·AI)은 가상 기기만 제어한다.
 * 사람이 직접 낸 명령(USER·BULK)과 시스템 안전 동작(SYSTEM)에는 적용하지 않는다.
 */
public final class SandboxGuard {

    private SandboxGuard() {
    }

    /** 막아야 하면 true */
    public static boolean forbidden(Set<Long> sandboxSpaceIds, SourceType sourceType, Long sourceSpaceId, boolean virtualDevice) {
        if (virtualDevice || sourceSpaceId == null) {
            return false;
        }
        return switch (sourceType) {
            case USER, BULK, SYSTEM -> false;
            default -> sandboxSpaceIds.contains(sourceSpaceId);
        };
    }
}
