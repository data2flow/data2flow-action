package net.java21.data2flow.action.actuation.domain;

import java.util.List;
import java.util.Map;

/**
 * 일괄 제어 규칙(BR-ACT-17, ACT-02.06): 대상 500대 이하, 대상 기기가 모두 그 기능을 지원해야 한다.
 */
public final class BulkPolicy {

    public static final int MAX_DEVICES = 500;

    /** 판정 */
    public record Check(boolean ok, String code, List<Long> unsupported) {
        static final Check OK = new Check(true, null, List.of());
    }

    private BulkPolicy() {
    }

    /**
     * @param devices        펼친 대상 기기
     * @param capabilityOf   기기 → 지원 기능 여부(제어할 수 없는 기기도 false)
     */
    public static Check check(List<Long> devices, Map<Long, Boolean> capabilityOf) {
        if (devices.size() > MAX_DEVICES) {
            return new Check(false, "COMMAND_BULK_LIMIT_EXCEEDED", List.of());
        }
        if (devices.isEmpty()) {
            return new Check(false, "COMMAND_BULK_EMPTY", List.of());
        }
        List<Long> unsupported = devices.stream().filter(d -> !Boolean.TRUE.equals(capabilityOf.get(d))).toList();
        return unsupported.isEmpty() ? Check.OK : new Check(false, "CAPABILITY_NOT_SUPPORTED", unsupported);
    }
}
