package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.contracts.sink.SinkMode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 쓰기 배치(SinkWriteRequest 하나, ≤ 1,000건). 레코드 키는 대상 열·필드 이름이다(노드가 매핑을 적용함, ADR-048).
 *
 * @param idempotencyKey 배치 멱등 키(64자). SQL 대상은 같은 트랜잭션에 표시 행을 남겨 한 번만 쓴다
 * @param target         테이블 또는 measurement
 * @param mode           INSERT·UPSERT
 * @param upsertKeys     UPSERT 키 열(InfluxDB는 태그)
 * @param records        레코드
 */
public record SinkBatch(String idempotencyKey, String target, SinkMode mode, List<String> upsertKeys,
                        List<Map<String, Object>> records) {

    public SinkBatch {
        upsertKeys = upsertKeys == null ? List.of() : List.copyOf(upsertKeys);
        records = records == null ? List.of() : List.copyOf(records);
    }

    /** 모든 레코드에 나온 열(처음 나온 순서) */
    public List<String> columns() {
        Set<String> cols = new LinkedHashSet<>();
        for (Map<String, Object> r : records) {
            cols.addAll(r.keySet());
        }
        return new ArrayList<>(cols);
    }
}
