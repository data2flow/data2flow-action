package net.java21.data2flow.action.sink.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.sink.SinkErrorCode;
import net.java21.data2flow.action.sink.SinkProperties;
import net.java21.data2flow.action.sink.connector.InMemorySinkConnector;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnectorRegistry;
import net.java21.data2flow.action.sink.connector.TargetSchema;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository.SinkBatchRow;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.secret.Secret;
import net.java21.data2flow.contracts.sink.SinkMode;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import net.java21.data2flow.contracts.web.CursorListApiResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** FLW-04.04 대상 스키마 확인·자동 생성, FLW-04.01 연결 테스트, dead-letter 목록(TC-FLW-086) */
class SinkNodeTest {

    private final MutableClock clock = new MutableClock(MutableClock.T0);
    private final InMemorySinkConnector memory = new InMemorySinkConnector();
    private final SinkConnectionClient client = mock(SinkConnectionClient.class);
    private final SinkBatchRepository repo = mock(SinkBatchRepository.class);
    private final SinkConnectorRegistry registry = new SinkConnectorRegistry(List.of(memory));
    private final SinkConnectionCache cache = new SinkConnectionCache(client, registry, clock, Duration.ofMinutes(5));
    private final SinkWriteService writer = new SinkWriteService(repo, mock(ExecutionRepository.class), cache, registry,
            SinkRetryPolicy.of(SinkProperties.defaults()), mock(PlatformTransactionManager.class), new SimpleMeterRegistry(), clock,
            "test", Duration.ofSeconds(60), Duration.ofDays(7));
    private final SinkConnectionService service = new SinkConnectionService(cache, registry, repo, writer);

    SinkNodeTest() {
        when(client.find(4)).thenReturn(Optional.of(new SinkConnection(4, 1, "MEMORY", Map.of(), Map.of(), 1)));
    }

    @Test
    @DisplayName("[FLW-04.04][AT-FLW-18.4][TC-FLW-086] 대상 테이블 없음 → 경고 근거(exists=false, 빠진 열), [자동 생성] 후 적용 가능")
    void missingTableThenAutoCreate() {
        SinkConnectionService.SchemaCheck before = service.schema(1, 4, "room_temp", List.of("device_id", "ts", "temperature"));
        assertThat(before.exists()).isFalse();
        assertThat(before.missingColumns()).containsExactly("device_id", "ts", "temperature");

        SinkConnectionService.SchemaCheck after = service.create(1, 4, "room_temp",
                List.of(new TargetSchema.Column("device_id", "INTEGER"), new TargetSchema.Column("ts", "TIMESTAMP"),
                        new TargetSchema.Column("temperature", "NUMBER")), List.of("device_id", "ts"));
        assertThat(after.exists()).isTrue();
        assertThat(after.missingColumns()).isEmpty();
        assertThat(service.schema(1, 4, "room_temp", List.of("humidity")).missingColumns()).containsExactly("humidity");
    }

    @Test
    @DisplayName("[FLW-04.01][TC-FLW-086] 다른 조직의 연결은 없는 것처럼(404), 잘못된 대상 이름은 400, 모르는 종류는 400")
    void scopeAndErrors() {
        assertThatThrownBy(() -> service.schema(2, 4, "room_temp", List.of())).isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode()).isEqualTo(SinkErrorCode.SINK_CONNECTION_NOT_FOUND);
        assertThatThrownBy(() -> service.schema(1, 4, "room temp", List.of())).isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode()).isEqualTo(SinkErrorCode.SINK_TARGET_INVALID);
        assertThatThrownBy(() -> service.testDraft(new SinkConnection(0, 1, "KAFKA", Map.of(), Map.of(), 0)))
                .isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode()).isEqualTo(SinkErrorCode.SINK_TYPE_NOT_SUPPORTED);
        assertThatThrownBy(() -> service.create(1, 4, "t", List.of(), List.of())).isInstanceOf(BusinessException.class);
    }

    @Test
    @DisplayName("[FLW-04.01][TC-FLW-086] 연결 테스트: 저장 전 본문·저장된 연결 모두 원인 종류로 결과를 돌려준다")
    void connectionTest() {
        assertThat(service.test(1, 4).ok()).isTrue();
        var auth = service.testDraft(new SinkConnection(0, 1, "MEMORY", Map.of(), Map.of("password", Secret.of("wrong")), 0));
        assertThat(auth.ok()).isFalse();
        assertThat(auth.errorKind().name()).isEqualTo("AUTH");
        // 연결 변경: 캐시와 풀을 지운다
        cache.invalidate(4);
        assertThat(memory.released).contains(4L);
    }

    @Test
    @DisplayName("[FLW-04.03][TC-FLW-086] dead-letter 목록은 최신순 커서로 이어 읽고, 재전송할 것이 없으면 404")
    void deadLetters() {
        SinkWriteRequest req = new SinkWriteRequest(4, "room_temp", SinkMode.INSERT, List.of(), List.of(Map.of("device_id", 15)), null);
        Instant t = MutableClock.T0;
        SinkBatchRow a = new SinkBatchRow("ka", 1, "r", 4, "room_temp", "INSERT", req, 1, "DEAD", 3, null, "x", "AUTH", t, null, t.plusSeconds(2),
                t.plusSeconds(86_402));
        SinkBatchRow b = new SinkBatchRow("kb", 1, "r", 4, "room_temp", "INSERT", req, 1, "DEAD", 3, null, "x", "AUTH", t, null, t.plusSeconds(1),
                t.plusSeconds(86_401));
        when(repo.findDead(eq(1L), eq(4L), isNull(), isNull(), eq(2))).thenReturn(List.of(a, b));
        CursorListApiResponse<SinkConnectionService.DeadLetter> first = service.deadLetters(1, 4, null, 1);
        assertThat(first.responses()).extracting(SinkConnectionService.DeadLetter::id).containsExactly("ka");
        assertThat(first.nextCursor()).isNotNull();
        when(repo.findDead(eq(1L), eq(4L), eq(t.plusSeconds(2)), eq("ka"), eq(2))).thenReturn(List.of(b));
        CursorListApiResponse<SinkConnectionService.DeadLetter> second = service.deadLetters(1, 4, first.nextCursor(), 1);
        assertThat(second.responses()).extracting(SinkConnectionService.DeadLetter::id).containsExactly("kb");
        assertThat(second.nextCursor()).isNull();
        assertThatThrownBy(() -> service.deadLetters(1, 4, "!!", 10)).isInstanceOf(BusinessException.class);

        when(repo.updateDeadToPending(eq(1L), eq(4L), anyList(), any())).thenReturn(0);
        assertThatThrownBy(() -> service.resend(1, 4, List.of("zz"), false)).isInstanceOf(BusinessException.class)
                .extracting(e -> ((BusinessException) e).getErrorCode()).isEqualTo(SinkErrorCode.SINK_DEAD_LETTER_NOT_FOUND);
        assertThatThrownBy(() -> service.resend(1, 4, List.of(), false)).isInstanceOf(BusinessException.class);
        when(repo.updateDeadToPending(eq(1L), eq(4L), isNull(), any())).thenReturn(2);
        assertThat(service.resend(1, 4, null, true)).isEqualTo(2);
    }
}
