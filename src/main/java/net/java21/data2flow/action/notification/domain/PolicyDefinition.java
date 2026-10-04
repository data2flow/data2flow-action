package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.alarm.AlarmSeverity;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 알림 정책(core API-RUL-40 = API-RUL-21 모양). action은 플로우 알림 노드의 정책 수신자 계산, 재알림 간격, 묶기 창, 해제 알림,
 * 에스컬레이션 단계에 쓴다.
 */
public record PolicyDefinition(long policyId, long organizationId, AlarmSeverity minSeverity, List<Member> recipients, List<String> channels,
                               int renotifyMinutes, int aggregateWindowSec, boolean notifyOnClear, List<Step> steps) {

    /** 에스컬레이션 최대 단계(BR-RUL-16) */
    public static final int MAX_STEPS = 3;

    public PolicyDefinition {
        recipients = recipients == null ? List.of() : List.copyOf(recipients);
        channels = channels == null ? List.of() : List.copyOf(channels);
        steps = steps == null ? List.of() : List.copyOf(steps);
        if (steps.size() > MAX_STEPS) {
            throw new IllegalArgumentException("에스컬레이션은 최대 3단계입니다: " + steps.size());
        }
    }

    /** 수신자 정의 {@code {type: USER|ROLE|ON_CALL, id}} */
    public record Member(String type, String id) {
    }

    /** 에스컬레이션 단계: 이 단계 수신자에게 보낸 뒤 {@code waitMinutes} 안에 확인이 없으면 다음 단계 */
    public record Step(int stepNo, int waitMinutes, List<Member> recipients) {
        public Step {
            recipients = recipients == null ? List.of() : List.copyOf(recipients);
        }
    }

    public Optional<Step> step(int stepNo) {
        return steps.stream().filter(s -> s.stepNo() == stepNo).findFirst();
    }

    public static PolicyDefinition from(JsonNode r) {
        List<Step> steps = new ArrayList<>();
        r.path("steps").forEach(s -> steps.add(new Step(s.path("stepNo").asInt(), s.path("waitMinutes").asInt(), members(s.path("recipients")))));
        List<String> channels = new ArrayList<>();
        r.path("channels").forEach(c -> channels.add(c.asString()));
        AlarmSeverity min;
        try {
            min = AlarmSeverity.valueOf(r.path("minSeverity").asString("INFO"));
        } catch (IllegalArgumentException e) {
            min = AlarmSeverity.INFO;
        }
        return new PolicyDefinition(r.path("notificationPolicyId").asLong(r.path("id").asLong()), r.path("organizationId").asLong(), min,
                members(r.path("recipients")), channels, r.path("renotifyMinutes").asInt(30), r.path("aggregateWindowSec").asInt(0),
                r.path("notifyOnClear").asBoolean(false), steps);
    }

    private static List<Member> members(JsonNode arr) {
        List<Member> out = new ArrayList<>();
        arr.forEach(m -> out.add(new Member(m.path("type").asString(), m.path("id").isMissingNode() || m.path("id").isNull() ? null
                : m.path("id").asString())));
        return out;
    }
}
