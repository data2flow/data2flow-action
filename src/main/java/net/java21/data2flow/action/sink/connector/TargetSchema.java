package net.java21.data2flow.action.sink.connector;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 대상 스키마(FLW-04.04, {@code {exists, columns:[{name,type}]}}).
 *
 * @param exists  대상(테이블·버킷)이 있는가
 * @param columns 열(스키마 없는 저장소는 빈 목록)
 * @param schemaless 열 검사가 의미 없는 저장소(InfluxDB)
 */
public record TargetSchema(boolean exists, List<Column> columns, boolean schemaless) {

    public TargetSchema {
        columns = columns == null ? List.of() : List.copyOf(columns);
    }

    public static TargetSchema missing() {
        return new TargetSchema(false, List.of(), false);
    }

    /** 대상에 없는 열(대소문자 무시). 대상이 없으면 전부, 스키마가 없는 저장소면 빈 목록 */
    public List<String> missingColumns(List<String> wanted) {
        if (schemaless && exists) {
            return List.of();
        }
        Set<String> have = columns.stream().map(c -> c.name().toLowerCase(Locale.ROOT)).collect(Collectors.toSet());
        return wanted.stream().filter(w -> !have.contains(w.toLowerCase(Locale.ROOT))).toList();
    }

    /**
     * 열 하나.
     *
     * @param name 이름
     * @param type 형식(STRING·NUMBER·INTEGER·BOOLEAN·TIMESTAMP 또는 저장소 원래 형식)
     */
    public record Column(String name, String type) {
    }
}
