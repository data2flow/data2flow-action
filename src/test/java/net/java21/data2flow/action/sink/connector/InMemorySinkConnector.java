package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.contracts.sink.SinkMode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 시험용 메모리 Sink 커넥터(종류 MEMORY). 키트를 통과하는 참조 구현이고, 단위 시험에서 장애({@link #failNext})를 흉내 낸다.
 * 연결 설정 {@code host=down}이면 REFUSED, {@code password=wrong}이면 AUTH, 호스트가 {@code .invalid}로 끝나면 DNS.
 */
@SinkConnectorVerified("InMemorySinkConnectorContractTest")
public class InMemorySinkConnector implements SinkConnector {

    public final Map<String, List<Map<String, Object>>> tables = new ConcurrentHashMap<>();
    public final Map<String, List<TargetSchema.Column>> schemas = new ConcurrentHashMap<>();
    public final Set<String> written = ConcurrentHashMap.newKeySet();
    public final AtomicInteger writes = new AtomicInteger();
    public final List<Long> released = new ArrayList<>();
    public volatile SinkWriteException failNext;
    public volatile int failTimes;

    @Override
    public String type() {
        return "MEMORY";
    }

    @Override
    public SinkTestResult test(SinkConnection c) {
        try {
            check(c);
            return SinkTestResult.ok(1);
        } catch (SinkWriteException e) {
            return SinkTestResult.failed(1, e.kind(), e.getMessage());
        }
    }

    @Override
    public TargetSchema describe(SinkConnection c, String target) throws SinkWriteException {
        check(c);
        Identifiers.table(target);
        List<TargetSchema.Column> cols = schemas.get(target);
        return cols == null ? TargetSchema.missing() : new TargetSchema(true, cols, false);
    }

    @Override
    public void create(SinkConnection c, String target, List<TargetSchema.Column> columns, List<String> primaryKey)
            throws SinkWriteException {
        check(c);
        Identifiers.table(target);
        schemas.putIfAbsent(target, List.copyOf(columns));
        tables.putIfAbsent(target, new ArrayList<>());
    }

    @Override
    public synchronized WriteOutcome write(SinkConnection c, SinkBatch batch) throws SinkWriteException {
        check(c);
        Identifiers.table(batch.target());
        if (failTimes > 0 && failNext != null) {
            failTimes--;
            throw failNext;
        }
        if (!schemas.containsKey(batch.target())) {
            throw new SinkWriteException(ErrorKind.TARGET, false, "테이블 없음: " + batch.target());
        }
        if (!written.add(batch.idempotencyKey())) {
            return WriteOutcome.ALREADY_WRITTEN;
        }
        writes.incrementAndGet();
        List<Map<String, Object>> rows = tables.get(batch.target());
        for (Map<String, Object> r : batch.records()) {
            if (batch.mode() == SinkMode.UPSERT) {
                rows.removeIf(old -> batch.upsertKeys().stream().allMatch(k -> java.util.Objects.equals(old.get(k), r.get(k))));
            }
            rows.add(new LinkedHashMap<>(r));
        }
        return WriteOutcome.WRITTEN;
    }

    @Override
    public void release(long connectionId) {
        released.add(connectionId);
    }

    private static void check(SinkConnection c) throws SinkWriteException {
        String host = c.string("host", "memory");
        if ("down".equals(host)) {
            throw new SinkWriteException(ErrorKind.REFUSED, true, "연결 거부");
        }
        if (host.endsWith(".invalid")) {
            throw new SinkWriteException(ErrorKind.DNS, true, "주소 없음");
        }
        if ("wrong".equals(c.secret("password"))) {
            throw new SinkWriteException(ErrorKind.AUTH, false, "인증 실패");
        }
    }

    public long count(String target) {
        return tables.getOrDefault(target, List.of()).size();
    }

    public Set<String> tableNames() {
        return new HashSet<>(tables.keySet());
    }
}
