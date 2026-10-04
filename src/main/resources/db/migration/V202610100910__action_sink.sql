-- Sink 쓰기 배치(FLW-04.02·04.03, BR-FLW-28, ADR-025·048). expand(추가만, ADR-030).
-- action.sinks 큐에서 받은 SinkWriteRequest를 배치 하나 = 행 하나로 저장하고 ACK한다. 쓰기는 이 행을 보고 하며, 일시 장애는 지수 백오프로
-- 다시 시도하고(대상 DB가 10분 멈춰도 유실 0, TC-FLW-084), 영구 실패·재시도 기한 초과는 DEAD(dead-letter, 24시간 보관 후 정리, 재전송 가능).
CREATE TABLE sink_batches (
    idempotency_key  varchar(64)   NOT NULL,
    organization_id  bigint        NOT NULL,
    request_key      varchar(64)   NOT NULL,
    connection_id    bigint        NOT NULL,
    target           varchar(128)  NOT NULL,
    mode             varchar(10)   NOT NULL,
    payload          jsonb         NOT NULL,
    record_count     integer       NOT NULL,
    status           varchar(10)   NOT NULL DEFAULT 'PENDING',
    attempts         integer       NOT NULL DEFAULT 0,
    next_retry_at    timestamptz,
    last_error       varchar(500),
    error_kind       varchar(10),
    created_at       timestamptz   NOT NULL DEFAULT now(),
    written_at       timestamptz,
    dead_at          timestamptz,
    dead_until       timestamptz,
    source           jsonb,
    env              varchar(8)    NOT NULL DEFAULT 'prod',
    CONSTRAINT pk_sink_batches PRIMARY KEY (idempotency_key),
    CONSTRAINT ck_sink_batches_status CHECK (status IN ('PENDING','RETRYING','WRITTEN','DEAD')),
    CONSTRAINT ck_sink_batches_mode CHECK (mode IN ('INSERT','UPSERT'))
);
-- 재시도 작업자: WHERE status IN ('PENDING','RETRYING') AND env = ? AND next_retry_at <= now() FOR UPDATE SKIP LOCKED
CREATE INDEX ix_sink_batches_due ON sink_batches (env, next_retry_at) WHERE status IN ('PENDING','RETRYING');
-- dead-letter 목록(커서, 최신순)과 24시간 정리
CREATE INDEX ix_sink_batches_dead ON sink_batches (organization_id, connection_id, dead_at DESC, idempotency_key) WHERE status = 'DEAD';
CREATE INDEX ix_sink_batches_written ON sink_batches (written_at) WHERE status = 'WRITTEN';
COMMENT ON TABLE sink_batches IS 'Sink 쓰기 배치(action.sinks). 일시 장애 재시도, 실패 배치 dead-letter 24시간 보관·재전송(BR-FLW-28)';
COMMENT ON COLUMN sink_batches.idempotency_key IS 'sha256("sink", 조직, 행동 요청 멱등 키). 대상 DB 표시 테이블 data2flow_sink_batches에도 같은 키(정확히 한 번)';
COMMENT ON COLUMN sink_batches.connection_id IS 'data2flow_core.sink_connections.id (스키마 간 FK 없음)';
COMMENT ON COLUMN sink_batches.dead_until IS 'dead-letter 보관 끝(DEAD 된 시각 + 24시간). 지나면 정리';
COMMENT ON COLUMN sink_batches.env IS '배포(dev·stg·prod). staging·prod DB 공유(ADR-030)에서 재시도 작업은 자기 배포 행만';
