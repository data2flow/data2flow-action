package net.java21.data2flow.action.sink.service;

import net.java21.data2flow.action.sink.SinkErrorCode;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnector;
import net.java21.data2flow.action.sink.connector.SinkConnectorRegistry;
import net.java21.data2flow.action.sink.connector.SinkTestResult;
import net.java21.data2flow.action.sink.connector.SinkWriteException;
import net.java21.data2flow.action.sink.connector.TargetSchema;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository.SinkBatchRow;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.FieldErrorDetail;
import net.java21.data2flow.contracts.web.CursorListApiResponse;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

/**
 * 연결 관리 보조(API-FLW-50·51, FLW-04.01·04.04): 연결 테스트(저장 전·저장된 연결), 대상 스키마 확인·자동 생성, dead-letter 목록·재전송.
 * 연결 정의·권한은 core가 보고 넘긴다. 여기서는 연결의 조직이 호출 신원의 조직과 같은지만 다시 본다(남의 것 404).
 */
public class SinkConnectionService {

    private final SinkConnectionCache cache;
    private final SinkConnectorRegistry connectors;
    private final SinkBatchRepository batches;
    private final SinkWriteService writer;

    public SinkConnectionService(SinkConnectionCache cache, SinkConnectorRegistry connectors, SinkBatchRepository batches,
                                 SinkWriteService writer) {
        this.cache = cache;
        this.connectors = connectors;
        this.batches = batches;
        this.writer = writer;
    }

    /** 저장 전 연결 테스트(본문의 종류·설정·비밀값) */
    public SinkTestResult testDraft(SinkConnection draft) {
        return connector(draft.type()).test(draft);
    }

    /** 저장된 연결 테스트 */
    public SinkTestResult test(long organizationId, long connectionId) {
        SinkConnection c = connection(organizationId, connectionId);
        return connector(c.type()).test(c);
    }

    /** 대상 스키마와 빠진 열(FLW-04.04 "테이블 room_temp 없음" 경고의 근거) */
    public SchemaCheck schema(long organizationId, long connectionId, String target, List<String> columns) {
        SinkConnection c = connection(organizationId, connectionId);
        try {
            TargetSchema s = connector(c.type()).describe(c, target);
            return new SchemaCheck(s.exists(), s.columns(), s.missingColumns(columns == null ? List.of() : columns));
        } catch (SinkWriteException e) {
            throw failure(e);
        }
    }

    /** [자동 생성]: 대상을 만든 뒤 다시 확인한 스키마 */
    public SchemaCheck create(long organizationId, long connectionId, String target, List<TargetSchema.Column> columns,
                              List<String> primaryKey) {
        if (columns == null || columns.isEmpty()) {
            throw new BusinessException(SinkErrorCode.SINK_TARGET_INVALID, List.of(), "만들 열이 없습니다");
        }
        SinkConnection c = connection(organizationId, connectionId);
        try {
            connector(c.type()).create(c, target, columns, primaryKey);
        } catch (SinkWriteException e) {
            throw failure(e);
        }
        return schema(organizationId, connectionId, target, columns.stream().map(TargetSchema.Column::name).toList());
    }

    /** dead-letter 목록(최신순 커서 목록, 기본 50·최대 500) */
    public CursorListApiResponse<DeadLetter> deadLetters(long organizationId, long connectionId, String cursor, Integer size) {
        int limit = size == null ? 50 : Math.clamp(size, 1, 500);
        Instant beforeAt = null;
        String beforeKey = null;
        if (cursor != null && !cursor.isBlank()) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8).split("\\|", 2);
                beforeAt = Instant.parse(parts[0]);
                beforeKey = parts[1];
            } catch (RuntimeException e) {
                throw new BusinessException(net.java21.data2flow.contracts.error.CommonErrorCode.INVALID_REQUEST,
                        List.of(new FieldErrorDetail("cursor", "INVALID", "커서가 올바르지 않습니다")));
            }
        }
        List<SinkBatchRow> rows = batches.findDead(organizationId, connectionId, beforeAt, beforeKey, limit + 1);
        boolean more = rows.size() > limit;
        List<SinkBatchRow> page = more ? rows.subList(0, limit) : rows;
        String next = null;
        if (more) {
            SinkBatchRow last = page.get(page.size() - 1);
            next = Base64.getUrlEncoder().withoutPadding().encodeToString((last.deadAt() + "|" + last.key()).getBytes(StandardCharsets.UTF_8));
        }
        return CursorListApiResponse.of(limit, page.stream().map(DeadLetter::of).toList(), next);
    }

    /** 재전송({@code ids} 또는 전체). 바뀐 건수 */
    public int resend(long organizationId, long connectionId, List<String> ids, boolean all) {
        if (!all && (ids == null || ids.isEmpty())) {
            throw new BusinessException(net.java21.data2flow.contracts.error.CommonErrorCode.INVALID_REQUEST,
                    List.of(new FieldErrorDetail("ids", "REQUIRED", "ids 또는 all=true가 필요합니다")));
        }
        int n = writer.resend(organizationId, connectionId, all ? null : ids);
        if (n == 0) {
            throw new BusinessException(SinkErrorCode.SINK_DEAD_LETTER_NOT_FOUND);
        }
        return n;
    }

    private SinkConnection connection(long organizationId, long connectionId) {
        return cache.find(connectionId).filter(c -> c.organizationId() == organizationId)
                .orElseThrow(() -> new BusinessException(SinkErrorCode.SINK_CONNECTION_NOT_FOUND));
    }

    private SinkConnector connector(String type) {
        return connectors.find(type).orElseThrow(() -> new BusinessException(SinkErrorCode.SINK_TYPE_NOT_SUPPORTED, List.of(),
                type));
    }

    private static BusinessException failure(SinkWriteException e) {
        if (e.kind() == net.java21.data2flow.action.sink.connector.ErrorKind.TARGET) {
            return new BusinessException(SinkErrorCode.SINK_TARGET_INVALID, List.of(), e.getMessage());
        }
        return new BusinessException(SinkErrorCode.SINK_CONNECTION_TEST_FAILED, List.of(), e.kind() + ": " + e.getMessage());
    }

    /** 스키마 확인 결과 {@code {exists, columns, missingColumns}} */
    public record SchemaCheck(boolean exists, List<TargetSchema.Column> columns, List<String> missingColumns) {
    }

    /** dead-letter 한 건 */
    public record DeadLetter(String id, String target, String mode, int recordCount, int attempts, String errorKind, String lastError,
                             Instant createdAt, Instant deadAt, Instant deadUntil) {
        static DeadLetter of(SinkBatchRow r) {
            return new DeadLetter(r.key(), r.target(), r.mode(), r.recordCount(), r.attempts(), r.errorKind(), r.lastError(),
                    r.createdAt(), r.deadAt(), r.deadUntil());
        }
    }
}
