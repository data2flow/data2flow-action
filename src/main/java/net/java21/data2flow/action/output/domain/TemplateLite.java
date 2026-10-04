package net.java21.data2flow.action.output.domain;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 로직 없는 Mustache 부분집합(출력 연결 format=TEMPLATE, DSC domain-model §2.9 "안전한 템플릿"). {@code {{이름}}}·{@code {{{이름}}}}만
 * 값으로 바꾸고(이스케이프 없음), 모르는 변수는 빈 문자열이다. 구역·부분 템플릿·람다는 없다(사용자 코드 실행 없음).
 */
public final class TemplateLite {

    private static final Pattern VAR = Pattern.compile("\\{\\{\\{?\\s*([A-Za-z][A-Za-z0-9_]*)\\s*}?}}");

    private TemplateLite() {
    }

    public static String render(String template, Map<String, String> values) {
        Matcher m = VAR.matcher(template);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            String v = values.get(m.group(1));
            m.appendReplacement(out, Matcher.quoteReplacement(v == null ? "" : v));
        }
        m.appendTail(out);
        return out.toString();
    }
}
