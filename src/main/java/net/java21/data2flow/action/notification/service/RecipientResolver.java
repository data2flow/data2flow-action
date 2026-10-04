package net.java21.data2flow.action.notification.service;

import net.java21.data2flow.action.notification.domain.AlarmInfo;
import net.java21.data2flow.action.notification.domain.ChannelDefinition;
import net.java21.data2flow.action.notification.domain.Delivery;
import net.java21.data2flow.action.notification.domain.OnCallSchedule;
import net.java21.data2flow.action.notification.domain.PolicyDefinition;
import net.java21.data2flow.action.notification.domain.RecipientProfile;
import net.java21.data2flow.contracts.notification.NotificationRecipient;
import net.java21.data2flow.contracts.notification.NotificationRequest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 수신자 계산(BR-RUL-12·19, TC-RUL-069·072·100). 채널과 무관하다: 채널 키는 문자열로만 다룬다(BR-OPS-32).
 *
 * <ol>
 *   <li>요청 수신자가 없으면 정책(API-RUL-40)의 수신자 × 채널로 만든다(플로우 {@code action.notify} 노드).</li>
 *   <li>USER·ROLE·ON_CALL을 사용자로 펼친다. 당직이 비어 있으면 같은 채널의 다른 사람 수신자가 있으면 그들만, 없으면 역할 ADMIN(BR-RUL-19).</li>
 *   <li>비활성 사용자는 뺀다. 알람 공간을 볼 수 없는 사용자는 보내지 않고 SKIPPED(NO_PERMISSION)로 남긴다(BR-RUL-12).</li>
 *   <li>메신저 채널은 사용자의 연결 계정이 주소다. 연결이 없으면 SKIPPED(NOT_LINKED). CHANNEL_DEFAULT는 채널의 기본 대화방.</li>
 *   <li>같은 채널·같은 사용자(또는 주소)는 한 번만(합치고 중복 제거).</li>
 * </ol>
 */
public class RecipientResolver {

    public static final String ROLE_ADMIN = "ADMIN";

    private final NotificationCoreClient core;

    public RecipientResolver(NotificationCoreClient core) {
        this.core = core;
    }

    /**
     * 받을 대상 하나.
     *
     * @param channelKey   채널 키(WEB 또는 SPI 키)
     * @param channel      채널 정의. WEB이면 null, 등록된 채널이 없으면 null이고 {@code error}가 있다
     * @param recipientKey 멱등 키·재알림 판정에 쓰는 수신자 표기(USER:5 또는 CHANNEL_DEFAULT@-100123)
     * @param address      채널 주소(텔레그램 chat_id). WEB이면 null
     * @param user         사용자(채널 기본 대화방이면 null)
     * @param skipReason   보내지 않을 사유(NO_PERMISSION·NOT_LINKED). 보내면 null
     * @param error        채널이 없을 때 FAILED 사유
     */
    public record Target(String channelKey, ChannelDefinition channel, String recipientKey, String address, RecipientProfile user,
                         String skipReason, String error) {

        public long channelId() {
            return channel == null ? 0L : channel.channelId();
        }
    }

    /** 요청의 수신자 정의. 정책만 있으면 정책 수신자 × 채널 */
    public static List<NotificationRecipient> requested(NotificationRequest n, PolicyDefinition policy) {
        if (!n.recipients().isEmpty() || policy == null) {
            return n.recipients();
        }
        return fromMembers(policy.recipients(), policy.channels());
    }

    /** 정책 수신자 정의 × 채널 */
    public static List<NotificationRecipient> fromMembers(List<PolicyDefinition.Member> members, List<String> channels) {
        List<NotificationRecipient> out = new ArrayList<>();
        for (String ch : channels.isEmpty() ? List.of(Delivery.WEB) : channels) {
            for (PolicyDefinition.Member m : members) {
                NotificationRecipient.Type type;
                try {
                    type = NotificationRecipient.Type.valueOf(m.type().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    continue;
                }
                out.add(new NotificationRecipient(type, m.id(), ch, null));
            }
        }
        return out;
    }

    public List<Target> resolve(long organizationId, List<NotificationRecipient> recipients, AlarmInfo alarm, Instant now) {
        // 1) 사람 수신자 펼치기: 채널별로 (사용자 ID 또는 역할) 목록
        Map<String, Set<Long>> usersByChannel = new LinkedHashMap<>();
        Map<String, Set<String>> rolesByChannel = new LinkedHashMap<>();
        List<Target> direct = new ArrayList<>();
        Map<String, Optional<ChannelDefinition>> channels = new LinkedHashMap<>();
        Set<String> onCallChannels = new LinkedHashSet<>();
        for (NotificationRecipient r : recipients) {
            String ch = r.channel().toUpperCase(Locale.ROOT);
            Optional<ChannelDefinition> def = channels.computeIfAbsent(ch, k -> Delivery.WEB.equals(k) ? Optional.empty()
                    : core.channels(organizationId, k).stream().findFirst());
            if (!Delivery.WEB.equals(ch) && def.isEmpty()) {
                direct.add(new Target(ch, null, r.recipientKey(), r.address(), null, null, "CHANNEL_NOT_CONFIGURED"));
                continue;
            }
            if (r.address() != null) {
                direct.add(new Target(ch, def.orElse(null), r.recipientKey(), r.address(), null, null, null));
                continue;
            }
            switch (r.type()) {
                case USER -> usersByChannel.computeIfAbsent(ch, k -> new LinkedHashSet<>()).add(Long.parseLong(r.id()));
                case ROLE -> rolesByChannel.computeIfAbsent(ch, k -> new LinkedHashSet<>()).add(r.id().toUpperCase(Locale.ROOT));
                case ON_CALL -> onCallChannels.add(ch);
                case CHANNEL_DEFAULT -> def.ifPresent(d -> d.defaultAddresses().forEach(a ->
                        direct.add(new Target(ch, d, "CHANNEL_DEFAULT@" + a, a, null, null, null))));
                default -> {
                    // 모르는 종류는 무시
                }
            }
        }
        if (!onCallChannels.isEmpty()) {
            Optional<Long> onCall = core.onCall(organizationId).onCallAt(now);
            for (String ch : onCallChannels) {
                if (onCall.isPresent()) {
                    usersByChannel.computeIfAbsent(ch, k -> new LinkedHashSet<>()).add(onCall.get());
                } else if (!usersByChannel.containsKey(ch) && !rolesByChannel.containsKey(ch)) {
                    rolesByChannel.computeIfAbsent(ch, k -> new LinkedHashSet<>()).add(ROLE_ADMIN);   // BR-RUL-19 마지막 대안
                }
            }
        }
        Set<Long> allUsers = new LinkedHashSet<>();
        usersByChannel.values().forEach(allUsers::addAll);
        Set<String> allRoles = new LinkedHashSet<>();
        rolesByChannel.values().forEach(allRoles::addAll);
        List<RecipientProfile> profiles = core.recipients(organizationId, allUsers, allRoles);

        // 2) 대상 만들기(채널·사용자 중복 제거)
        Map<String, Target> out = new LinkedHashMap<>();
        for (Target t : direct) {
            out.putIfAbsent(t.channelKey() + "|" + t.recipientKey(), t);
        }
        Set<String> chs = new LinkedHashSet<>(usersByChannel.keySet());
        chs.addAll(rolesByChannel.keySet());
        for (String ch : chs) {
            Set<Long> ids = usersByChannel.getOrDefault(ch, Set.of());
            Set<String> roles = rolesByChannel.getOrDefault(ch, Set.of());
            ChannelDefinition def = channels.get(ch).orElse(null);
            for (RecipientProfile p : profiles) {
                boolean wanted = ids.contains(p.userId()) || roles.contains(p.role().toUpperCase(Locale.ROOT));
                if (!wanted || !p.active()) {
                    continue;
                }
                String key = "USER:" + p.userId();
                String skip = null;
                String address = null;
                if (alarm != null && !p.canSee(alarm.spacePathIds())) {
                    skip = SKIP_NO_PERMISSION;
                } else if (!Delivery.WEB.equals(ch)) {
                    address = p.links().get(ch);
                    if (address == null || address.isBlank()) {
                        skip = SKIP_NOT_LINKED;
                    }
                }
                out.putIfAbsent(ch + "|" + key, new Target(ch, def, key, address, p, skip, null));
            }
        }
        return List.copyOf(out.values());
    }

    /** 공간 권한이 없는 수신자(BR-RUL-12) */
    public static final String SKIP_NO_PERMISSION = "NO_PERMISSION";
    /** 메신저 계정이 연결되지 않은 수신자 */
    public static final String SKIP_NOT_LINKED = "NOT_LINKED";
}
