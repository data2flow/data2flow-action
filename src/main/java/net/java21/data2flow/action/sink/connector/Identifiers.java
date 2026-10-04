package net.java21.data2flow.action.sink.connector;

import java.util.regex.Pattern;

/**
 * 대상 이름(테이블·열·measurement) 검사. SQL에 이름을 붙여 넣으므로 허용 문자만 받고 따옴표로 감싼다(SQL 주입 방지).
 * 허용: 영문자·숫자·밑줄, 영문자나 밑줄로 시작, 1~63자. 테이블은 {@code 스키마.테이블} 한 단계까지.
 */
public final class Identifiers {

    private static final Pattern NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    private Identifiers() {
    }

    public static boolean valid(String name) {
        return name != null && NAME.matcher(name).matches();
    }

    /** 이름 검사. 틀리면 TARGET 영구 실패 */
    public static String require(String name) throws SinkWriteException {
        if (!valid(name)) {
            throw new SinkWriteException(ErrorKind.TARGET, false, "허용하지 않는 이름입니다: " + name);
        }
        return name;
    }

    /** 대상 이름({@code 테이블} 또는 {@code 스키마.테이블}) → [스키마|null, 테이블] */
    public static String[] table(String target) throws SinkWriteException {
        if (target == null) {
            throw new SinkWriteException(ErrorKind.TARGET, false, "대상이 없습니다");
        }
        String[] parts = target.split("\\.", -1);
        if (parts.length == 1) {
            return new String[]{null, require(parts[0])};
        }
        if (parts.length == 2) {
            return new String[]{require(parts[0]), require(parts[1])};
        }
        throw new SinkWriteException(ErrorKind.TARGET, false, "허용하지 않는 대상 이름입니다: " + target);
    }
}
