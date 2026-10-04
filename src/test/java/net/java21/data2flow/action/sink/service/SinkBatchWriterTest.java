package net.java21.data2flow.action.sink.service;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.sink.SinkProperties;
import net.java21.data2flow.action.sink.connector.ErrorKind;
import net.java21.data2flow.action.sink.connector.InMemorySinkConnector;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnectorRegistry;
import net.java21.data2flow.action.sink.connector.SinkWriteException;
import net.java21.data2flow.action.sink.connector.TargetSchema;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository.SinkBatchRow;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.sink.SinkMode;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * BR-FLW-28 규칙 표(TC-FLW-085): Sink 쓰기는 배치와 재시도를 거치고, 실패 레코드는 dead-letter에 24시간 보관한 뒤 재전송할 수 있다.
 * 배치 크기(100건·1초)는 노드가 정해 요청 하나로 보내므로(SinkWriteRequest.batches) 여기서는 재시도·dead-letter·보관·재전송을 본다.
 */
class SinkBatchWriterTest {

    private static final SinkRetryPolicy POLICY = SinkRetryPolicy.of(SinkProperties.defaults());
    private static final Instant T0 = MutableClock.T0;

    private final MutableClock clock = new MutableClock(T0);
    private final SinkBatchRepository repo = mock(SinkBatchRepository.class);
    private final ExecutionRepository executions = mock(ExecutionRepository.class);
    private final SinkConnectionClient client = mock(SinkConnectionClient.class);
    private final InMemorySinkConnector memory = new InMemorySinkConnector();
    private SinkWriteService service;

    @BeforeEach
    void setUp() throws Exception {
        SinkConnectorRegistry registry = new SinkConnectorRegistry(List.of(memory));
        SinkConnectionCache cache = new SinkConnectionCache(client, registry, clock, Duration.ofMinutes(5));
        when(client.find(4)).thenReturn(Optional.of(new SinkConnection(4, 1, "MEMORY", Map.of(), Map.of(), 1)));
        memory.create(new SinkConnection(4, 1, "MEMORY", Map.of(), Map.of(), 1), "room_temp",
                List.of(new TargetSchema.Column("device_id", "INTEGER")), List.of());
        service = new SinkWriteService(repo, executions, cache, registry, POLICY, mock(PlatformTransactionManager.class),
                new SimpleMeterRegistry(), clock, "test", Duration.ofSeconds(60), Duration.ofDays(7));
    }

    @ParameterizedTest(name = "{0}번 실패 뒤 {1}ms 대기")
    @CsvSource({"1, 1000", "2, 2000", "3, 4000", "6, 32000", "7, 60000", "20, 60000"})
    @DisplayName("[FLW-04.03][TC-FLW-085] 일시 장애 재시도 간격: 1초부터 2배, 최대 60초")
    void backoff(int attempts, long millis) {
        assertThat(POLICY.backoff(attempts)).isEqualTo(Duration.ofMillis(millis));
    }

    @ParameterizedTest(name = "{0}·{1}·경과 {2}분 → 재시도 {3}")
    @CsvSource({
            "REFUSED, true, 10, true",      // 대상 DB 10분 중단: 계속 다시 시도(유실 0, TC-FLW-084)
            "TIMEOUT, true, 359, true",
            "REFUSED, true, 361, false",    // 6시간이 지나면 dead-letter
            "AUTH, false, 0, false",        // 인증 실패는 바로 dead-letter
            "TARGET, false, 0, false"})     // 대상·열 없음, 형식 오류
    @DisplayName("[FLW-04.03][TC-FLW-085] BR-FLW-28 재시도·dead-letter 판정")
    void retryOrDead(ErrorKind kind, boolean transientFailure, long ageMinutes, boolean retry) {
        assertThat(POLICY.retry(new SinkWriteException(kind, transientFailure, "x"), T0, T0.plus(Duration.ofMinutes(ageMinutes))))
                .isEqualTo(retry);
    }

    @Test
    @DisplayName("[FLW-04.03][TC-FLW-085] dead-letter는 24시간 보관한다")
    void deadRetention() {
        assertThat(POLICY.deadUntil(T0)).isEqualTo(T0.plus(Duration.ofHours(24)));
    }

    private SinkBatchRow row(String key, int attempts) {
        SinkWriteRequest req = new SinkWriteRequest(4, "room_temp", SinkMode.INSERT, List.of(),
                List.of(Map.of("device_id", 15), Map.of("device_id", 16)), null);
        return new SinkBatchRow(key, 1, "req", 4, "room_temp", "INSERT", req, 2, "PENDING", attempts, T0, null, null, T0, null, null, null);
    }

    @Test
    @DisplayName("[FLW-04.02][TC-FLW-085] 받은 배치는 바로 쓰고 WRITTEN으로 기록한다")
    void writesImmediately() {
        when(repo.lockForWrite(eq(1L), eq("k1"), any(), any())).thenReturn(Optional.of(row("k1", 0)));
        service.writeNow(1, "k1");
        assertThat(memory.count("room_temp")).isEqualTo(2);
        verify(repo).markWritten(eq(1L), eq("k1"), eq(1), any());
    }

    @Test
    @DisplayName("[FLW-04.03][TC-FLW-085] 일시 장애면 RETRYING(1초 뒤), 영구 실패면 DEAD(24시간 보관)")
    void transientRetriesPermanentDies() {
        memory.failNext = new SinkWriteException(ErrorKind.REFUSED, true, "연결 거부");
        memory.failTimes = 1;
        when(repo.lockDue(any(), any(), anyInt(), eq("test"))).thenReturn(List.of(row("k2", 0)));
        assertThat(service.processDue(10)).isEqualTo(1);
        verify(repo).markRetry(1, "k2", 1, T0.plusSeconds(1), "REFUSED", "연결 거부");

        memory.failNext = new SinkWriteException(ErrorKind.AUTH, false, "인증 실패");
        memory.failTimes = 1;
        when(repo.lockDue(any(), any(), anyInt(), eq("test"))).thenReturn(List.of(row("k3", 2)));
        service.processDue(10);
        verify(repo).markDead(1, "k3", 3, T0, T0.plus(Duration.ofHours(24)), "AUTH", "인증 실패");
        assertThat(memory.count("room_temp")).isZero();
    }

    @Test
    @DisplayName("[FLW-04.03][TC-FLW-085] 연결 정의가 없거나 다른 조직이면 DEAD, core 장애는 일시 실패로 다시 시도")
    void missingConnection() {
        when(client.find(4)).thenReturn(Optional.empty());
        when(repo.lockDue(any(), any(), anyInt(), eq("test"))).thenReturn(List.of(row("k4", 0)));
        service.processDue(10);
        verify(repo).markDead(eq(1L), eq("k4"), eq(1), any(), any(), eq("TARGET"), anyString());

        when(client.find(4)).thenThrow(new IllegalStateException("core 내부 API 실패: HTTP 503"));
        when(repo.lockDue(any(), any(), anyInt(), eq("test"))).thenReturn(List.of(row("k5", 0)));
        service.processDue(10);
        verify(repo).markRetry(eq(1L), eq("k5"), eq(1), eq(T0.plusSeconds(1)), eq("OTHER"), anyString());
    }

    @Test
    @DisplayName("[FLW-04.03][TC-FLW-085] 재전송은 DEAD → PENDING, 보관 정리는 dead-letter 24시간·쓴 배치 7일")
    void resendAndPurge() {
        when(repo.updateDeadToPending(1, 4, List.of("k6"), T0)).thenReturn(1);
        assertThat(service.resend(1, 4, List.of("k6"))).isEqualTo(1);
        service.purge();
        verify(repo).deleteExpired(T0, T0.minus(Duration.ofDays(7)));
    }

    @Test
    @DisplayName("[FLW-04.02][NFR-02.11] 같은 멱등 키의 요청은 한 번만 받고, SINK가 아닌 요청은 형식 오류(DLQ)")
    void acceptOnce() {
        SinkWriteRequest body = new SinkWriteRequest(4, "room_temp", SinkMode.INSERT, List.of(), List.of(Map.of("device_id", 15)), null);
        String key = ActionIdempotencyKeys.flow("f-1", "n-sink", "m-1");
        ActionRequest req = ActionRequest.sink(1, key, CommandSource.flow("f-1", 3, "n-sink", "m-1"), null, body, clock);
        when(executions.recordExecuted(anyLong(), anyString(), eq("SINK"), anyString(), eq("ACCEPTED"), any())).thenReturn(true, false);
        assertThat(service.accept(req)).contains(SinkWriteService.batchKey(1, key));
        assertThat(service.accept(req)).isEmpty();
        verify(repo).insert(eq(1L), eq(SinkWriteService.batchKey(1, key)), eq(key), any(), anyString(), eq(T0), eq("test"));

        ActionRequest notify = ActionRequest.command(1, key, CommandSource.flow("f-1", 3, "n-sink", "m-1"), null,
                new net.java21.data2flow.contracts.command.CommandPayload(
                        net.java21.data2flow.contracts.command.CommandTarget.device(15), "Switch", "set", Map.of("on", true), false), clock);
        assertThatThrownBy(() -> service.accept(notify)).isInstanceOf(MessageFormatException.class);
        verify(repo, never()).markDead(anyLong(), eq("x"), anyInt(), any(), any(), any(), any());
    }
}
