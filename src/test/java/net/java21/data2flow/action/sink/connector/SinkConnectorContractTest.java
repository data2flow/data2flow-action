package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.contracts.sink.SinkMode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sink 커넥터 계약 키트(FLW-04.05, TC-FLW-087). 모든 {@link SinkConnector} 구현은 이 키트를 상속한 {@code *SinkConnectorContractTest}를
 * 통과해야 {@link SinkConnectorVerified}를 붙여 등록할 수 있다. 키트가 확인하는 시나리오:
 * <ol>
 *   <li>종류 이름과 등록 표시</li>
 *   <li>배치 100건 쓰기, 같은 멱등 키 재호출은 중복 없음(정확히 한 번)</li>
 *   <li>UPSERT: 같은 키 레코드는 바뀌고 늘지 않음</li>
 *   <li>스키마 확인: 있는 대상·없는 대상·빠진 열</li>
 *   <li>연결 테스트 원인 매핑: 성공, 인증 실패 AUTH, 주소 없음 DNS, 닫힌 포트 REFUSED(또는 TIMEOUT) — 예외 없이 결과로</li>
 *   <li>허용하지 않는 이름(SQL 주입 시도)은 TARGET 영구 실패</li>
 * </ol>
 */
public abstract class SinkConnectorContractTest {

    /** 키트 표준 열: 기기 ID·시각(키), 온도, 표시 */
    public static final List<TargetSchema.Column> COLUMNS = List.of(new TargetSchema.Column("device_id", "INTEGER"),
            new TargetSchema.Column("ts", "TIMESTAMP"), new TargetSchema.Column("temperature", "NUMBER"),
            new TargetSchema.Column("label", "STRING"));
    public static final List<String> KEYS = List.of("device_id", "ts");
    private static final Instant T0 = Instant.parse("2026-03-02T00:00:00Z");

    protected abstract SinkConnector connector();

    /** 정상 연결 */
    protected abstract SinkConnection connection();

    protected abstract SinkConnection withWrongCredentials();

    protected abstract SinkConnection withUnknownHost();

    protected abstract SinkConnection withClosedPort();

    /** 대상의 행·점 수 */
    protected abstract long count(String target) throws Exception;

    /** 스키마가 없는 저장소(InfluxDB)면 true: 열 검사·없는 대상 판정을 건너뛴다 */
    protected boolean schemaless() {
        return false;
    }

    protected String newTarget() {
        return "kit_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    protected String prepare() throws Exception {
        String target = newTarget();
        connector().create(connection(), target, COLUMNS, KEYS);
        return target;
    }

    protected static List<Map<String, Object>> records(int n, double temperature) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("device_id", 15 + (i % 5));
            r.put("ts", T0.plusSeconds(i).toString());
            r.put("temperature", temperature + i * 0.1);
            r.put("label", "실습실 " + i);
            out.add(r);
        }
        return out;
    }

    protected static String key() {
        return UUID.randomUUID().toString().replace("-", "") + UUID.randomUUID().toString().replace("-", "");
    }

    @Test
    @DisplayName("[FLW-04.05][TC-FLW-087] 종류 이름이 있고 계약 키트 통과 표시가 붙어 있다")
    void typeIsDeclared() {
        assertThat(connector().type()).matches("[A-Z][A-Z0-9_]{1,31}");
        assertThat(connector().getClass().isAnnotationPresent(SinkConnectorVerified.class)).isTrue();
        assertThat(connector().getClass().getAnnotation(SinkConnectorVerified.class).value()).isEqualTo(getClass().getSimpleName());
    }

    @Test
    @DisplayName("[FLW-04.02][TC-FLW-087] 배치 100건을 쓰고, 같은 멱등 키로 다시 써도 중복되지 않는다")
    void writesBatchOf100Once() throws Exception {
        String target = prepare();
        SinkBatch batch = new SinkBatch(key(), target, SinkMode.INSERT, List.of(), records(100, 24.0));
        assertThat(connector().write(connection(), batch)).isEqualTo(SinkConnector.WriteOutcome.WRITTEN);
        assertThat(count(target)).isEqualTo(100);
        SinkConnector.WriteOutcome again = connector().write(connection(), batch);
        if (!schemaless()) {
            assertThat(again).isEqualTo(SinkConnector.WriteOutcome.ALREADY_WRITTEN);
        }
        assertThat(count(target)).isEqualTo(100);
    }

    @Test
    @DisplayName("[FLW-04.02][TC-FLW-087] UPSERT: 같은 키 레코드는 바뀌고 늘어나지 않는다")
    void upsertReplaces() throws Exception {
        String target = prepare();
        connector().write(connection(), new SinkBatch(key(), target, SinkMode.UPSERT, KEYS, records(10, 24.0)));
        connector().write(connection(), new SinkBatch(key(), target, SinkMode.UPSERT, KEYS, records(10, 27.0)));
        assertThat(count(target)).isEqualTo(10);
    }

    @Test
    @DisplayName("[FLW-04.04][TC-FLW-087] 스키마 확인: 있는 대상은 열을 보여 주고, 없는 대상·열은 빠졌다고 알린다")
    void schemaCheck() throws Exception {
        String target = prepare();
        TargetSchema s = connector().describe(connection(), target);
        assertThat(s.exists()).isTrue();
        if (!schemaless()) {
            assertThat(s.missingColumns(List.of("temperature", "humidity"))).containsExactly("humidity");
            assertThat(connector().describe(connection(), newTarget()).exists()).isFalse();
        }
    }

    @Test
    @DisplayName("[FLW-04.01][TC-FLW-087] 연결 테스트는 예외 없이 원인을 돌려준다: 성공·AUTH·DNS·REFUSED")
    void testMapsCauses() {
        assertThat(connector().test(connection()).ok()).isTrue();
        SinkTestResult auth = connector().test(withWrongCredentials());
        assertThat(auth.ok()).isFalse();
        assertThat(auth.errorKind()).isEqualTo(ErrorKind.AUTH);
        assertThat(connector().test(withUnknownHost()).errorKind()).isEqualTo(ErrorKind.DNS);
        assertThat(connector().test(withClosedPort()).errorKind()).isIn(ErrorKind.REFUSED, ErrorKind.TIMEOUT);
    }

    @Test
    @DisplayName("[FLW-04.02][TC-FLW-087] 허용하지 않는 대상 이름(SQL 주입 시도)은 영구 실패 TARGET")
    void rejectsUnsafeIdentifiers() {
        SinkBatch bad = new SinkBatch(key(), "room_temp; DROP TABLE x", SinkMode.INSERT, List.of(), records(1, 20));
        assertThatThrownBy(() -> connector().write(connection(), bad)).isInstanceOf(SinkWriteException.class)
                .satisfies(e -> {
                    assertThat(((SinkWriteException) e).kind()).isEqualTo(ErrorKind.TARGET);
                    assertThat(((SinkWriteException) e).transientFailure()).isFalse();
                });
    }
}
