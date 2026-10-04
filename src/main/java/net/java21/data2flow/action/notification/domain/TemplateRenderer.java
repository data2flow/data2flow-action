package net.java21.data2flow.action.notification.domain;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 알림 템플릿 변수 치환(RUL-03.04, TC-RUL-078): {@code {{device.name}}}은 변수의 점 경로(중첩 맵) 또는 점이 든 키 그대로를 찾는다.
 * 없는 변수는 빈 문자열이다. 숫자는 불필요한 소수점 0을 뺀다.
 */
public final class TemplateRenderer {

    private static final Pattern VAR = Pattern.compile("\\{\\{\\s*([A-Za-z0-9_.]+)\\s*}}");

    private TemplateRenderer() {
    }

    public static String render(String template, Map<String, Object> variables) {
        if (template == null) {
            return "";
        }
        Matcher m = VAR.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            m.appendReplacement(out, Matcher.quoteReplacement(format(lookup(variables, m.group(1)))));
        }
        m.appendTail(out);
        return out.toString();
    }

    static Object lookup(Map<String, Object> variables, String path) {
        if (variables == null) {
            return null;
        }
        if (variables.containsKey(path)) {
            return variables.get(path);
        }
        Object current = variables;
        for (String part : path.split("\\.")) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(part);
        }
        return current;
    }

    static String format(Object v) {
        if (v == null) {
            return "";
        }
        if (v instanceof Double d && d == Math.rint(d) && !d.isInfinite()) {
            return Long.toString(d.longValue());
        }
        if (v instanceof Number n) {
            return java.math.BigDecimal.valueOf(n.doubleValue()).stripTrailingZeros().toPlainString();
        }
        return v.toString();
    }
}
