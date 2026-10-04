package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.notification.NotificationFixtures;
import net.java21.data2flow.action.notification.domain.AlarmInfo;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.OnCallSchedule;
import net.java21.data2flow.action.notification.domain.PolicyDefinition;
import net.java21.data2flow.action.notification.domain.RecipientProfile;
import net.java21.data2flow.action.notification.service.RecipientResolver.Target;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.notification.NotificationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecipientResolverTest {

    private static final Instant NOW = Instant.parse("2026-03-02T12:00:00Z");   // 월 21:00 서울
    private final NotificationCoreClient core = mock(NotificationCoreClient.class);
    private final RecipientResolver resolver = new RecipientResolver(core);
    private final AlarmInfo alarm = AlarmInfo.from(Json.MAPPER.valueToTree(NotificationFixtures.alarm(9001, "MAJOR", "ACTIVE")));
    private final List<RecipientProfile> profiles = new ArrayList<>();

    @BeforeEach
    void setUp() {
        when(core.channels(eq(1L), eq("TELEGRAM"))).thenReturn(List.of(ChannelDefinition.from(Json.MAPPER.valueToTree(
                NotificationFixtures.telegramChannel(4, 20, 60)))));
        when(core.channels(eq(1L), eq("SLACK"))).thenReturn(List.of());
        when(core.onCall(anyLong())).thenReturn(OnCallSchedule.EMPTY);
        when(core.recipients(anyLong(), any(), any())).thenAnswer(inv -> {
            java.util.Collection<Long> ids = inv.getArgument(1);
            java.util.Collection<String> roles = inv.getArgument(2);
            return profiles.stream().filter(p -> ids.contains(p.userId()) || roles.contains(p.role())).toList();
        });
        profiles.add(profile(5, "OPERATOR", "555", true));
        profiles.add(profile(6, "OPERATOR", null, true));
        profiles.add(profile(7, "ADMIN", "777", true));
        profiles.add(profile(8, "OPERATOR", "888", false));   // 공간 31 권한 없음
    }

    private static RecipientProfile profile(long id, String role, String telegram, boolean scopeOk) {
        Map<String, Object> u = new LinkedHashMap<>(NotificationFixtures.user(id, role, telegram));
        u.put("spaceScope", Map.of("unrestricted", false, "allowedSpaceIds", scopeOk ? List.of(7) : List.of(99)));
        return RecipientProfile.from(Json.MAPPER.valueToTree(u));
    }

    private List<Target> resolve(List<NotificationRecipient> recipients) {
        return resolver.resolve(1, recipients, alarm, NOW);
    }

    @ParameterizedTest(name = "[RUL-03.02][BR-RUL-12][TC-RUL-069] {0} → {1}")
    @CsvSource(delimiter = '|', value = {
            "USER:5:TELEGRAM            | TELEGRAM/USER:5@555",
            "USER:6:TELEGRAM            | TELEGRAM/USER:6!NOT_LINKED",
            "USER:8:TELEGRAM            | TELEGRAM/USER:8!NO_PERMISSION",
            "USER:5:WEB                 | WEB/USER:5",
            "ROLE:ADMIN:TELEGRAM        | TELEGRAM/USER:7@777",
            "CHANNEL_DEFAULT::TELEGRAM  | TELEGRAM/CHANNEL_DEFAULT@-100777@-100777",
            "USER:5:SLACK               | SLACK/USER:5#CHANNEL_NOT_CONFIGURED",
            "ON_CALL::TELEGRAM          | TELEGRAM/USER:7@777"})
    void table(String recipient, String expected) {
        String[] p = recipient.trim().split(":", -1);
        NotificationRecipient r = new NotificationRecipient(NotificationRecipient.Type.valueOf(p[0]), p[1].isEmpty() ? null : p[1], p[2], null);
        assertThat(resolve(List.of(r))).extracting(RecipientResolverTest::describe).containsExactly(expected.trim());
    }

    @Test
    @DisplayName("[RUL-03.02][AT-RUL-08.1][TC-RUL-072] 정책 2개가 겹쳐도(USER 5 + ROLE OPERATOR) 한 번만, 공간 권한 없는 8은 발송 제외, 비활성은 뺀다")
    void unionDedupAndScope() {
        profiles.add(RecipientProfile.from(Json.MAPPER.valueToTree(Map.of("userId", "9", "role", "OPERATOR", "active", false))));
        List<Target> targets = resolve(List.of(NotificationRecipient.user(5, "TELEGRAM"),
                new NotificationRecipient(NotificationRecipient.Type.ROLE, "OPERATOR", "TELEGRAM", null),
                NotificationRecipient.user(5, "WEB")));
        assertThat(targets).extracting(RecipientResolverTest::describe).containsExactlyInAnyOrder(
                "TELEGRAM/USER:5@555", "TELEGRAM/USER:6!NOT_LINKED", "TELEGRAM/USER:8!NO_PERMISSION", "WEB/USER:5");
    }

    @Test
    @DisplayName("[RUL-05.03][BR-RUL-19][TC-RUL-100] 당직이 비면 같은 채널의 다른 수신자가 있으면 그들만, 없으면 조직 ADMIN. 당직이 있으면 당직자")
    void onCallFallback() {
        assertThat(resolve(List.of(NotificationRecipient.onCall("TELEGRAM"), NotificationRecipient.user(5, "TELEGRAM"))))
                .extracting(RecipientResolverTest::describe).containsExactly("TELEGRAM/USER:5@555");
        when(core.onCall(anyLong())).thenReturn(OnCallSchedule.from(Json.MAPPER.valueToTree(Map.of("timezone", "Asia/Seoul",
                "shifts", List.of(), "overrides", List.of(Map.of("startsAt", "2026-03-02T00:00:00Z", "endsAt", "2026-03-03T00:00:00Z",
                        "substituteUserId", "5"))))));
        assertThat(resolve(List.of(NotificationRecipient.onCall("TELEGRAM")))).extracting(RecipientResolverTest::describe)
                .containsExactly("TELEGRAM/USER:5@555");
    }

    @Test
    @DisplayName("[RUL-03.02][TC-RUL-069] 수신자가 없는 플로우 알림은 정책 수신자 × 채널로 만든다")
    void policyRecipients() {
        PolicyDefinition policy = PolicyDefinition.from(Json.MAPPER.valueToTree(Map.of("notificationPolicyId", 3, "minSeverity", "INFO",
                "recipients", List.of(Map.of("type", "USER", "id", "5"), Map.of("type", "ROLE", "id", "ADMIN")),
                "channels", List.of("TELEGRAM", "WEB"))));
        NotificationRequest n = new NotificationRequest(null, "flow.notify", null, null, 3L, List.of(), null, null, null, null, null, null, null);
        List<NotificationRecipient> rs = RecipientResolver.requested(n, policy);
        assertThat(rs).hasSize(4);
        assertThat(resolve(rs)).extracting(RecipientResolverTest::describe).containsExactlyInAnyOrder(
                "TELEGRAM/USER:5@555", "TELEGRAM/USER:7@777", "WEB/USER:5", "WEB/USER:7");
    }

    static String describe(Target t) {
        String s = t.channelKey() + "/" + t.recipientKey();
        if (t.error() != null) {
            return s + "#" + t.error();
        }
        if (t.skipReason() != null) {
            return s + "!" + t.skipReason();
        }
        return t.address() == null ? s : s + "@" + t.address();
    }
}
