package net.java21.data2flow.action.sink.service;

import io.micrometer.core.instrument.MeterRegistry;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.action.sink.connector.ErrorKind;
import net.java21.data2flow.action.sink.connector.SinkBatch;
import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnector;
import net.java21.data2flow.action.sink.connector.SinkConnectorRegistry;
import net.java21.data2flow.action.sink.connector.SinkWriteException;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository;
import net.java21.data2flow.action.sink.repository.SinkBatchRepository.SinkBatchRow;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.ActionKind;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.sink.SinkWriteRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Sink 실행(FLW-04.02·04.03, ADR-025): 행동 요청 kind=SINK를 받아 배치 행으로 저장하고, 커넥터로 대상 저장소에 쓴다.
 *
 * <ol>
 *   <li>{@link #accept}: 멱등 판정(executed_actions kind=SINK, 행동 요청은 영구 기록 BR-ACT-02) → {@code sink_batches} 행(PENDING) — 같은
 *       트랜잭션. 커밋 뒤 큐에 ACK한다(유실 0).</li>
 *   <li>{@link #writeNow}·{@link #processDue}: 행을 잠가 임대한 뒤 트랜잭션 밖에서 쓰고, 결과를 기록한다. 커넥터는 대상 DB 표시 행으로 같은 배치를
 *       한 번만 쓴다(정확히 한 번).</li>
 *   <li>일시 장애 → RETRYING(지수 백오프 1초~60초, {@link SinkRetryPolicy}), 영구 실패·재시도 기한 초과 → DEAD(dead-letter 24시간,
 *       재전송 가능, BR-FLW-28).</li>
 * </ol>
 */
public class SinkWriteService {

    public static final String CONSUMER = "action.sinks";
    private static final Logger log = LoggerFactory.getLogger(SinkWriteService.class);

    private final SinkBatchRepository batches;
    private final ExecutionRepository executions;
    private final SinkConnectionCache connections;
    private final SinkConnectorRegistry connectors;
    private final SinkRetryPolicy retry;
    private final TransactionTemplate tx;
    private final MeterRegistry meters;
    private final Clock clock;
    private final String env;
    private final Duration lease;
    private final Duration writtenRetention;

    public SinkWriteService(SinkBatchRepository batches, ExecutionRepository executions, SinkConnectionCache connections,
                            SinkConnectorRegistry connectors, SinkRetryPolicy retry, PlatformTransactionManager txManager,
                            MeterRegistry meters, Clock clock, String env, Duration lease, Duration writtenRetention) {
        this.batches = batches;
        this.executions = executions;
        this.connections = connections;
        this.connectors = connectors;
        this.retry = retry;
        this.tx = new TransactionTemplate(txManager);
        this.meters = meters;
        this.clock = clock;
        this.env = env;
        this.lease = lease;
        this.writtenRetention = writtenRetention;
    }

    /** 배치 저장 키: {@code sha256("sink", 조직, 행동 요청 키)} */
    public static String batchKey(long organizationId, String requestKey) {
        return ActionIdempotencyKeys.of("sink", Long.toString(organizationId), requestKey);
    }

    /**
     * 행동 요청을 받는다. 처음이면 배치 키, 이미 받은 요청이면 빈 값. 형식 오류는 {@link MessageFormatException}(재시도 없이 DLQ).
     */
    public Optional<String> accept(ActionRequest req) {
        if (req.kind() != ActionKind.SINK) {
            throw new MessageFormatException("SINK가 아닌 행동 요청입니다: " + req.kind());
        }
        SinkWriteRequest body = req.sinkWriteRequest();
        String key = batchKey(req.organizationId(), req.idempotencyKey());
        Instant now = clock.instant();
        Boolean first = tx.execute(s -> {
            executions.markProcessed(CONSUMER, req.messageId().toString(), now);
            if (!executions.recordExecuted(req.organizationId(), key, ActionKind.SINK.name(), key.substring(0, 32), "ACCEPTED", now)) {
                return false;   // 같은 멱등 키(재전달·재발행): 처음 결과를 따른다
            }
            batches.insert(req.organizationId(), key, req.idempotencyKey(), body, Json.write(req.source()), now, env);
            return true;
        });
        meters.counter("data2flow_action_sink_requests_total", "result", Boolean.TRUE.equals(first) ? "accepted" : "duplicate").increment();
        return Boolean.TRUE.equals(first) ? Optional.of(key) : Optional.empty();
    }

    /** 받은 배치를 바로 써 본다(실패하면 재시도 작업이 이어받는다) */
    public void writeNow(long organizationId, String key) {
        Instant now = clock.instant();
        Optional<SinkBatchRow> row = tx.execute(s -> batches.lockForWrite(organizationId, key, now, now.plus(lease)));
        if (row != null) {
            row.ifPresent(this::write);
        }
    }

    /** 재시도 작업: 차례가 된 배치를 쓴다. 처리한 건수 */
    public int processDue(int limit) {
        Instant now = clock.instant();
        List<SinkBatchRow> due = tx.execute(s -> batches.lockDue(now, now.plus(lease), limit, env));
        if (due == null) {
            return 0;
        }
        due.forEach(this::write);
        return due.size();
    }

    /** 보관 정리(dead-letter 24시간, 쓴 배치 7일) */
    public int purge() {
        Instant now = clock.instant();
        return batches.deleteExpired(now, now.minus(writtenRetention));
    }

    /** dead-letter 재전송. 바뀐 건수 */
    public int resend(long organizationId, long connectionId, List<String> keys) {
        Instant now = clock.instant();
        Integer n = tx.execute(s -> batches.updateDeadToPending(organizationId, connectionId, keys, now));
        return n == null ? 0 : n;
    }

    private void write(SinkBatchRow row) {
        int attempts = row.attempts() + 1;
        try {
            SinkConnection connection = connections.find(row.connectionId())
                    .filter(c -> c.organizationId() == row.organizationId())
                    .orElseThrow(() -> new SinkWriteException(ErrorKind.TARGET, false, "저장소 연결이 없습니다: " + row.connectionId()));
            SinkConnector connector = connectors.find(connection.type())
                    .orElseThrow(() -> new SinkWriteException(ErrorKind.TARGET, false, "지원하지 않는 저장소 종류입니다: " + connection.type()));
            SinkWriteRequest r = row.request();
            SinkConnector.WriteOutcome outcome = connector.write(connection,
                    new SinkBatch(row.key(), r.target(), r.mode(), r.upsertKeys(), r.records()));
            tx.executeWithoutResult(s -> batches.markWritten(row.organizationId(), row.key(), attempts, clock.instant()));
            meters.counter("data2flow_action_sink_batches_total", "type", connection.type(), "result", outcome.name()).increment();
            meters.counter("data2flow_action_sink_records_total", "type", connection.type()).increment(row.recordCount());
        } catch (SinkWriteException e) {
            failed(row, attempts, e);
        } catch (RuntimeException e) {
            // core 장애(연결 정의를 못 읽음) 등: 일시 실패로 본다
            failed(row, attempts, new SinkWriteException(ErrorKind.OTHER, true, e.toString(), e));
        }
    }

    private void failed(SinkBatchRow row, int attempts, SinkWriteException e) {
        Instant now = clock.instant();
        if (retry.retry(e, row.createdAt(), now)) {
            Instant next = now.plus(retry.backoff(attempts));
            tx.executeWithoutResult(s -> batches.markRetry(row.organizationId(), row.key(), attempts, next, e.kind().name(), e.getMessage()));
            log.info("Sink 쓰기 일시 실패(다시 시도 {}) key={} connection={} kind={}: {}", next, row.key(), row.connectionId(), e.kind(),
                    e.getMessage());
            meters.counter("data2flow_action_sink_failures_total", "kind", e.kind().name(), "result", "retry").increment();
        } else {
            tx.executeWithoutResult(s -> batches.markDead(row.organizationId(), row.key(), attempts, now, retry.deadUntil(now),
                    e.kind().name(), e.getMessage()));
            log.warn("Sink 배치를 dead-letter로 보냅니다 key={} connection={} kind={}: {}", row.key(), row.connectionId(), e.kind(),
                    e.getMessage());
            meters.counter("data2flow_action_sink_failures_total", "kind", e.kind().name(), "result", "dead").increment();
        }
    }
}
