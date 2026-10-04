-- 출력 연결 발송(DSC-04.01, BR-DSC-19, ADR-048). expand(추가만, ADR-030).
-- 연결 정의는 core(data2flow_core.output_connections, 내부 API-DSC-73)에 있고, action은 data2flow.telemetry를 소비자 그룹 action-output으로
-- 따로 읽어(수집과 분리) 연결마다 이 표에 쌓은 뒤(DB 커밋 후 오프셋 저장) 연결마다 순서대로 보낸다. 실패는 지수 백오프로 24시간까지
-- 다시 시도하고, 그 뒤에는 FAILED(실패 보관함, 재전송 API-DSC-77)로 둔다. 보낸 행·실패 행은 7일 뒤 지운다.
CREATE TABLE output_deliveries (
    id                bigint        GENERATED ALWAYS AS IDENTITY,
    organization_id   bigint        NOT NULL,
    output_id         bigint        NOT NULL,
    env               varchar(8)    NOT NULL,
    message_id        uuid          NOT NULL,
    part              varchar(64)   NOT NULL DEFAULT '',
    device_id         bigint        NOT NULL,
    measured_at       timestamptz   NOT NULL,
    topic             varchar(512),
    body              text          NOT NULL,
    status            varchar(8)    NOT NULL DEFAULT 'PENDING',
    attempts          integer       NOT NULL DEFAULT 0,
    next_attempt_at   timestamptz   NOT NULL,
    retry_started_at  timestamptz   NOT NULL,
    last_error        varchar(500),
    failure_kind      varchar(12),
    created_at        timestamptz   NOT NULL DEFAULT now(),
    sent_at           timestamptz,
    failed_at         timestamptz,
    CONSTRAINT pk_output_deliveries PRIMARY KEY (id),
    CONSTRAINT uq_output_deliveries_message UNIQUE (output_id, message_id, part),
    CONSTRAINT ck_output_deliveries_status CHECK (status IN ('PENDING','SENT','FAILED'))
);
-- 발송 작업자: 연결마다 맨 앞(가장 작은 id)의 대기 행부터
CREATE INDEX ix_output_deliveries_pending ON output_deliveries (env, output_id, id) WHERE status = 'PENDING';
-- 실패 보관함 재전송(API-DSC-77)과 정리
CREATE INDEX ix_output_deliveries_failed ON output_deliveries (organization_id, output_id, created_at) WHERE status = 'FAILED';
CREATE INDEX ix_output_deliveries_created ON output_deliveries (created_at) WHERE status <> 'PENDING';
COMMENT ON TABLE output_deliveries IS '출력 연결 발송 대기열(연결마다 순서대로). 24시간 재시도 후 FAILED 보관, 7일 뒤 정리(BR-DSC-19)';
COMMENT ON COLUMN output_deliveries.output_id IS 'data2flow_core.output_connections.id (스키마 간 FK 없음)';
COMMENT ON COLUMN output_deliveries.part IS '측정값마다 나눠 보낼 때 측정 항목 키, 아니면 빈 문자열. (output_id, message_id, part)로 한 번만 쌓는다(멱등)';
COMMENT ON COLUMN output_deliveries.retry_started_at IS '24시간 재시도 기한의 시작(쌓은 시각, 재전송하면 그 시각)';
COMMENT ON COLUMN output_deliveries.env IS '배포(dev·stg·prod). staging·prod DB 공유(ADR-030)에서 발송 작업은 자기 배포 행만';

-- 연결마다 발송 작업자 하나(파드 여러 개에서도 순서 유지). 리스가 끝나면 다른 파드가 넘겨받는다
CREATE TABLE output_sender_leases (
    output_id         bigint        NOT NULL,
    env               varchar(8)    NOT NULL,
    organization_id   bigint        NOT NULL,
    holder            varchar(100)  NOT NULL,
    lease_until       timestamptz   NOT NULL,
    CONSTRAINT pk_output_sender_leases PRIMARY KEY (output_id, env)
);
COMMENT ON TABLE output_sender_leases IS '출력 연결 발송 리스(연결·배포마다 한 파드만 보내 순서를 지킨다)';
