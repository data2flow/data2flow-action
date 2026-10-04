package net.java21.data2flow.action.notification.domain;

import net.java21.data2flow.contracts.notification.NotificationEvents;

import java.util.Map;

/**
 * 내장 문구(ADR-037: ko 원문, en·ja·zh, 없으면 ja·zh → en → ko). core 템플릿(API-RUL-45)이 없을 때의 기본 템플릿과 버튼·요약·안내 문구.
 */
public final class NotificationTexts {

    private NotificationTexts() {
    }

    /** 기본 템플릿 {제목, 본문} */
    public static String[] defaultTemplate(String event, String locale) {
        String l = lang(locale);
        return switch (event == null ? "" : event) {
            case NotificationEvents.ALARM_CLEARED -> switch (l) {
                case "en" -> new String[]{"[Cleared] {{title}}", "{{title}} has been cleared.\n{{space}} {{device.name}}"};
                case "ja" -> new String[]{"[解除] {{title}}", "{{title}} が解除されました。\n{{space}} {{device.name}}"};
                case "zh" -> new String[]{"[已解除] {{title}}", "{{title}} 已解除。\n{{space}} {{device.name}}"};
                default -> new String[]{"[해제] {{title}}", "{{title}} 알람이 해제되었습니다.\n{{space}} {{device.name}}"};
            };
            case NotificationEvents.FLOW_NOTIFY -> new String[]{"{{title}}", "{{message}}"};
            default -> switch (l) {
                case "en" -> new String[]{"[{{severity}}] {{title}}", "{{title}}\n{{space}} {{device.name}}\nValue: {{value}}"};
                case "ja" -> new String[]{"[{{severity}}] {{title}}", "{{title}}\n{{space}} {{device.name}}\n値: {{value}}"};
                case "zh" -> new String[]{"[{{severity}}] {{title}}", "{{title}}\n{{space}} {{device.name}}\n数值: {{value}}"};
                default -> new String[]{"[{{severity}}] {{title}}", "{{title}}\n{{space}} {{device.name}}\n값: {{value}}"};
            };
        };
    }

    private static final Map<String, Map<String, String>> TEXTS = Map.ofEntries(
            Map.entry("ack", Map.of("ko", "확인", "en", "Acknowledge", "ja", "確認", "zh", "确认")),
            Map.entry("mute", Map.of("ko", "30분 무음", "en", "Mute 30 min", "ja", "30分ミュート", "zh", "静音30分钟")),
            Map.entry("digest", Map.of("ko", "같은 알람 {0}건, 대표: {1}", "en", "{0} similar alarms, e.g. {1}", "ja", "同じアラーム{0}件、代表: {1}",
                    "zh", "相同告警 {0} 条，代表：{1}")),
            Map.entry("digestEarlier", Map.of("ko", "(앞서 {0}건 발송)", "en", "({0} sent earlier)", "ja", "(先に{0}件送信)", "zh", "（此前已发送 {0} 条）")),
            Map.entry("acked", Map.of("ko", "✔ 확인됨", "en", "✔ Acknowledged", "ja", "✔ 確認済み", "zh", "✔ 已确认")),
            Map.entry("muted", Map.of("ko", "🔕 30분 무음", "en", "🔕 Muted for 30 min", "ja", "🔕 30分ミュート", "zh", "🔕 已静音30分钟")),
            Map.entry("notLinked", Map.of("ko", "data2flow 계정이 연결되지 않았습니다. 웹의 내 정보 > 메신저 연결에서 연결한 뒤 다시 눌러 주세요.",
                    "en", "Your data2flow account is not linked. Link it in My profile > Messenger, then try again.",
                    "ja", "data2flowアカウントが連携されていません。マイページ > メッセンジャー連携で連携してから再度押してください。",
                    "zh", "尚未绑定 data2flow 账号。请在 我的信息 > 即时通讯绑定 中绑定后重试。")),
            Map.entry("linked", Map.of("ko", "data2flow 계정이 연결되었습니다.", "en", "Your data2flow account is now linked.",
                    "ja", "data2flowアカウントを連携しました。", "zh", "已绑定 data2flow 账号。")),
            Map.entry("linkFailed", Map.of("ko", "연결 코드가 없거나 만료되었습니다. 웹에서 다시 시작해 주세요.",
                    "en", "The link code is invalid or expired. Start again from the web.",
                    "ja", "連携コードが無効か期限切れです。Webからやり直してください。", "zh", "绑定码无效或已过期，请在网页上重新开始。")),
            Map.entry("unsupported", Map.of("ko", "이 버튼은 아직 처리할 수 없습니다.", "en", "This button is not supported yet.",
                    "ja", "このボタンはまだ処理できません。", "zh", "暂不支持此按钮。")),
            Map.entry("test", Map.of("ko", "data2flow 테스트 메시지입니다.", "en", "data2flow test message.", "ja", "data2flowのテストメッセージです。",
                    "zh", "这是 data2flow 测试消息。")),
            Map.entry("virtual", Map.of("ko", "[가상] ", "en", "[Virtual] ", "ja", "[仮想] ", "zh", "[虚拟] ")));

    /** 문구 하나({0}·{1} 치환) */
    public static String text(String key, String locale, Object... args) {
        Map<String, String> byLang = TEXTS.get(key);
        String l = lang(locale);
        String t = byLang.getOrDefault(l, byLang.getOrDefault("en", byLang.get("ko")));
        for (int i = 0; i < args.length; i++) {
            t = t.replace("{" + i + "}", String.valueOf(args[i]));
        }
        return t;
    }

    static String lang(String locale) {
        if (locale == null || locale.isBlank()) {
            return "ko";
        }
        String l = locale.toLowerCase(java.util.Locale.ROOT);
        return l.length() > 2 ? l.substring(0, 2) : l;
    }
}
