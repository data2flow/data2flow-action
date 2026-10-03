-- data2flow_action 초기 스키마(ACT-01.01·02.01~02.05·03.01·03.02·06.01·06.04, TSD-01.03). 정본: data2flow-docs design/erd/action.md, ddl/22-action.sql
-- ERD 초안과 다른 점(action M3 구현에서 추가, ERD에 반영함): commands.timeout_at·ix_commands_timeout(명령 기한 작업),
-- commands.env·outboxes.env(배포 구분: staging·prod DB 공유에서 각 배포가 자기 행만 처리, ADR-030)
-- 스키마는 Flyway(create-schemas)가 만들고, 검색 경로는 default-schema(data2flow_action)다.

-- -----------------------------------------------------------------------------
-- Command
-- -----------------------------------------------------------------------------
CREATE TABLE commands (
    id                    uuid          NOT NULL,
    organization_id       bigint        NOT NULL,
    idempotency_key       char(64)      NOT NULL,
    device_id             bigint        NOT NULL,
    capability            varchar(64)   NOT NULL,
    command               varchar(64)   NOT NULL,
    args                  jsonb         NOT NULL DEFAULT '{}'::jsonb,
    priority              varchar(10)   NOT NULL,
    source                jsonb         NOT NULL,
    source_type           varchar(12)   GENERATED ALWAYS AS ((source ->> 'type')::varchar(12)) STORED,
    source_ref            varchar(64)   GENERATED ALWAYS AS (
                              (COALESCE(source ->> 'sceneRunId', source ->> 'bulkJobId', source ->> 'flowId'))::varchar(64)
                          ) STORED,
    status                varchar(24)   NOT NULL DEFAULT 'REQUESTED',
    status_reason         varchar(32),
    valid_until           timestamptz   NOT NULL,
    execute_after         timestamptz,
    expected_delivery_at  timestamptz,
    attempts              integer       NOT NULL DEFAULT 0,
    driver_response       jsonb,
    requested_at          timestamptz   NOT NULL DEFAULT now(),
    sent_at               timestamptz,
    acked_at              timestamptz,
    applied_at            timestamptz,
    finished_at           timestamptz,
    timeout_at            timestamptz,
    env                   varchar(8)    NOT NULL DEFAULT 'prod',
    CONSTRAINT pk_commands PRIMARY KEY (id),
    CONSTRAINT uq_commands_org_idempotency_key UNIQUE (organization_id, idempotency_key),
    CONSTRAINT ck_commands_priority CHECK (priority IN ('MANUAL','SAFETY','SCHEDULE','AUTO','AI')),
    CONSTRAINT ck_commands_source_type CHECK (source_type IN ('USER','FLOW','RULE','AI','SCHEDULE','SCENE','BULK','SYSTEM')),
    CONSTRAINT ck_commands_status CHECK (status IN (
        'REQUESTED','REJECTED','BLOCKED','SKIPPED','DELAYED','QUEUED','QUEUED_FOR_DOWNLINK',
        'SENT','ACKED','APPLIED','TIMEOUT','FAILED','SUPERSEDED','CANCELLED'))
);
CREATE INDEX ix_commands_org_device_requested ON commands (organization_id, device_id, requested_at DESC, id);
CREATE INDEX ix_commands_org_requested ON commands (organization_id, requested_at DESC, id);
CREATE INDEX ix_commands_active ON commands (device_id, capability)
    WHERE status IN ('REQUESTED','DELAYED','QUEUED','QUEUED_FOR_DOWNLINK','SENT','ACKED');
CREATE INDEX ix_commands_due ON commands (execute_after) WHERE status = 'DELAYED';
-- 응답·적용·대기 기한(SENT → TIMEOUT_ACK, ACKED → TIMEOUT_APPLY, QUEUED → EXPIRED)과 재시도 시각(REQUESTED)을 한 열로 본다
CREATE INDEX ix_commands_timeout ON commands (timeout_at)
    WHERE status IN ('REQUESTED','QUEUED','SENT','ACKED');
CREATE INDEX ix_commands_source_ref ON commands (source_type, source_ref) WHERE source_ref IS NOT NULL;
COMMENT ON TABLE commands IS '제어 창구(Control Facade)를 거친 명령. 화면·플로우·AI·예약 모두 여기로 모임(ADR-009)';
COMMENT ON COLUMN commands.idempotency_key IS '사용자: Idempotency-Key, 플로우: sha256(flowId,nodeId,triggerMessageId)(BR-FLW-13)';
COMMENT ON COLUMN commands.priority IS '출처가 정하는 우선순위(BR-ACT-24)';
COMMENT ON COLUMN commands.device_id IS 'data2flow_core.devices.id (스키마 간 FK 없음)';
COMMENT ON COLUMN commands.valid_until IS '대기열 유효 시간. 지나면 FAILED(EXPIRED)';
COMMENT ON COLUMN commands.env IS '명령을 받은 배포(dev·stg·prod). staging·prod가 DB를 함께 쓰므로(ADR-030) 기한·재시도 작업은 자기 배포 행만 본다';
COMMENT ON COLUMN commands.timeout_at IS '지금 상태의 기한: REQUESTED=드라이버 (재)호출 시각, SENT=ack 기한, ACKED=적용 기한, QUEUED=valid_until';

CREATE TABLE command_events (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    command_id       uuid          NOT NULL,
    organization_id  bigint        NOT NULL,
    at               timestamptz   NOT NULL DEFAULT now(),
    from_status      varchar(24),
    to_status        varchar(24)   NOT NULL,
    reason           varchar(32),
    detail           jsonb,
    CONSTRAINT pk_command_events PRIMARY KEY (id),
    CONSTRAINT fk_command_events_command FOREIGN KEY (command_id) REFERENCES commands (id) ON DELETE CASCADE
);
CREATE INDEX ix_command_events_command_at ON command_events (command_id, at);
COMMENT ON TABLE command_events IS '명령 상태 전이 타임라인';

CREATE TABLE bulk_jobs (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    requested_by     bigint        NOT NULL,
    target           jsonb         NOT NULL,
    capability       varchar(64)   NOT NULL,
    command          varchar(64)   NOT NULL,
    args             jsonb         NOT NULL DEFAULT '{}'::jsonb,
    total            integer       NOT NULL DEFAULT 0,
    succeeded        integer       NOT NULL DEFAULT 0,
    failed           integer       NOT NULL DEFAULT 0,
    queued           integer       NOT NULL DEFAULT 0,
    status           varchar(10)   NOT NULL DEFAULT 'RUNNING',
    created_at       timestamptz   NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CONSTRAINT pk_bulk_jobs PRIMARY KEY (id),
    CONSTRAINT ck_bulk_jobs_status CHECK (status IN ('RUNNING','COMPLETED'))
);
CREATE INDEX ix_bulk_jobs_org_created ON bulk_jobs (organization_id, created_at DESC);
COMMENT ON TABLE bulk_jobs IS '일괄 제어 작업. 개별 명령은 commands.source.bulkJobId로 연결';

CREATE TABLE scene_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    scene_id         bigint        NOT NULL,
    source           jsonb         NOT NULL,
    status           varchar(10)   NOT NULL DEFAULT 'RUNNING',
    results          jsonb         NOT NULL DEFAULT '[]'::jsonb,
    started_at       timestamptz   NOT NULL DEFAULT now(),
    finished_at      timestamptz,
    CONSTRAINT pk_scene_runs PRIMARY KEY (id),
    CONSTRAINT ck_scene_runs_status CHECK (status IN ('RUNNING','SUCCEEDED','PARTIAL','FAILED'))
);
CREATE INDEX ix_scene_runs_org_scene_started ON scene_runs (organization_id, scene_id, started_at DESC);
COMMENT ON COLUMN scene_runs.scene_id IS 'data2flow_core.scenes.id (스키마 간 FK 없음)';

CREATE TABLE executed_actions (
    idempotency_key  char(64)      NOT NULL,
    organization_id  bigint        NOT NULL,
    kind             varchar(12)   NOT NULL,
    result_ref       varchar(64),
    result_status    varchar(24)   NOT NULL,
    executed_at      timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_executed_actions PRIMARY KEY (idempotency_key),
    CONSTRAINT ck_executed_actions_kind CHECK (kind IN ('COMMAND','NOTIFY','SINK','SCENE','WORK_ORDER','ANALYSIS','EVENT'))
);
COMMENT ON TABLE executed_actions IS '모든 행동의 멱등 기록. INSERT ... ON CONFLICT DO NOTHING 후 처음 결과 반환. 영구 보관(BR-ACT-02)';

-- -----------------------------------------------------------------------------
-- DeviceShadow
-- -----------------------------------------------------------------------------
CREATE TABLE device_shadows (
    device_id           bigint        NOT NULL,
    organization_id     bigint        NOT NULL,
    desired             jsonb         NOT NULL DEFAULT '{}'::jsonb,
    desired_version     bigint        NOT NULL DEFAULT 0,
    desired_updated_at  timestamptz,
    desired_source      jsonb,
    reported            jsonb         NOT NULL DEFAULT '{}'::jsonb,
    reported_version    bigint        NOT NULL DEFAULT 0,
    reported_at         timestamptz,
    delta               jsonb         NOT NULL DEFAULT '{}'::jsonb,
    connectivity        varchar(8)    NOT NULL DEFAULT 'UNKNOWN',
    updated_at          timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_device_shadows PRIMARY KEY (device_id),
    CONSTRAINT ck_device_shadows_connectivity CHECK (connectivity IN ('UNKNOWN','ONLINE','OFFLINE'))
);
CREATE INDEX ix_device_shadows_org ON device_shadows (organization_id);
COMMENT ON TABLE device_shadows IS '기기의 원하는 상태(desired)·보고된 상태(reported)·차이(delta)';
COMMENT ON COLUMN device_shadows.reported_version IS '이보다 작은 보고는 버림(BR-ACT-05)';

CREATE TABLE device_state_history (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    device_id        bigint        NOT NULL,
    capability       varchar(64)   NOT NULL,
    attribute        varchar(64)   NOT NULL,
    value            jsonb         NOT NULL,
    label            varchar(64),
    valid_from       timestamptz   NOT NULL,
    valid_to         timestamptz,
    source           jsonb,
    command_id       uuid,
    CONSTRAINT pk_device_state_history PRIMARY KEY (id, valid_from),
    CONSTRAINT ck_device_state_history_range CHECK (valid_to IS NULL OR valid_to >= valid_from)
) PARTITION BY RANGE (valid_from);
CREATE TABLE device_state_history_default PARTITION OF device_state_history DEFAULT;
CREATE TABLE device_state_history_y2026m10 PARTITION OF device_state_history
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');
CREATE INDEX ix_device_state_history_device_from ON device_state_history (device_id, capability, valid_from DESC);
COMMENT ON TABLE device_state_history IS '액추에이터 상태 구간(API-TSD-05 actuator). 월 파티션, 스케줄러가 생성·정리. 컬럼은 API 응답에서 도출(확인 필요)';

CREATE TABLE manual_overrides (
    device_id        bigint        NOT NULL,
    capability       varchar(64)   NOT NULL,
    organization_id  bigint        NOT NULL,
    until            timestamptz   NOT NULL,
    set_by           bigint        NOT NULL,
    command_id       uuid,
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_manual_overrides PRIMARY KEY (device_id, capability)
);
COMMENT ON TABLE manual_overrides IS '수동 우선. until까지 자동 명령은 SKIPPED(MANUAL_OVERRIDE)';

CREATE TABLE protection_state (
    device_id        bigint        NOT NULL,
    organization_id  bigint        NOT NULL,
    last_on_at       timestamptz,
    last_off_at      timestamptz,
    cycles_today     integer       NOT NULL DEFAULT 0,
    cycles_date      date,
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_protection_state PRIMARY KEY (device_id)
);
COMMENT ON TABLE protection_state IS '장비 보호(최소 간격·일 사이클) 판정 상태(BR-ACT-07)';

CREATE TABLE runtime_stat_daily (
    device_id            bigint            NOT NULL,
    day                  date              NOT NULL,
    organization_id      bigint            NOT NULL,
    on_seconds           integer           NOT NULL DEFAULT 0,
    cycles               integer           NOT NULL DEFAULT 0,
    energy_wh_estimated  double precision,
    energy_source        varchar(10),
    no_effect_events     integer           NOT NULL DEFAULT 0,
    CONSTRAINT pk_runtime_stat_daily PRIMARY KEY (device_id, day),
    CONSTRAINT ck_runtime_stat_daily_energy_source CHECK (energy_source IS NULL OR energy_source IN ('RATED','REPORTED'))
);
CREATE INDEX ix_runtime_stat_daily_org_day ON runtime_stat_daily (organization_id, day);
COMMENT ON COLUMN runtime_stat_daily.day IS '사이트 시간대 기준 날짜';

-- -----------------------------------------------------------------------------
-- NotificationDelivery
-- -----------------------------------------------------------------------------
CREATE TABLE notification_deliveries (
    id                   uuid          NOT NULL,
    organization_id      bigint        NOT NULL,
    idempotency_key      char(64)      NOT NULL,
    channel_id           bigint        NOT NULL,
    channel_type         varchar(20)   NOT NULL,
    source_type          varchar(20)   NOT NULL,
    source_id            varchar(64),
    alarm_id             bigint,
    aggregate_id         bigint,
    recipients           jsonb         NOT NULL,
    subject              varchar(200),
    body_preview         varchar(200),
    digest_count         integer       NOT NULL DEFAULT 1,
    status               varchar(10)   NOT NULL DEFAULT 'PENDING',
    skip_reason          varchar(20),
    attempt              smallint      NOT NULL DEFAULT 0,
    next_retry_at        timestamptz,
    last_error           varchar(500),
    external_message_id  varchar(128),
    created_at           timestamptz   NOT NULL DEFAULT now(),
    sent_at              timestamptz,
    CONSTRAINT pk_notification_deliveries PRIMARY KEY (id),
    CONSTRAINT uq_notification_deliveries_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_notification_deliveries_source_type CHECK (source_type IN ('ALARM','FLOW','REPORT','SYSTEM','TEST')),
    CONSTRAINT ck_notification_deliveries_status CHECK (status IN ('PENDING','RETRYING','SENT','FAILED','SKIPPED','DIGESTED')),
    CONSTRAINT ck_notification_deliveries_skip_reason CHECK (skip_reason IS NULL OR skip_reason IN ('SILENCED','DND','FLAPPING','SUPPRESSED','RENOTIFY_INTERVAL')),
    CONSTRAINT ck_notification_deliveries_attempt CHECK (attempt BETWEEN 0 AND 5)
);
CREATE INDEX ix_notification_deliveries_org_created ON notification_deliveries (organization_id, created_at DESC, id);
-- 재시도 작업자: WHERE status IN (...) AND next_retry_at <= now() FOR UPDATE SKIP LOCKED
CREATE INDEX ix_notification_deliveries_retry ON notification_deliveries (next_retry_at) WHERE status IN ('PENDING','RETRYING');
CREATE INDEX ix_notification_deliveries_alarm ON notification_deliveries (alarm_id) WHERE alarm_id IS NOT NULL;
COMMENT ON TABLE notification_deliveries IS '알림 발송 기록(RUL·OPS 정의 통합). 채널은 지금 텔레그램(ADR-033)';
COMMENT ON COLUMN notification_deliveries.idempotency_key IS 'sha256(alarmId, eventType, recipient, channel, eventSeq)(BR-RUL-17)';
COMMENT ON COLUMN notification_deliveries.channel_id IS 'data2flow_core.notification_channels.id (스키마 간 FK 없음)';

CREATE TABLE webhook_deliveries (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    webhook_id       bigint        NOT NULL,
    event_id         varchar(64)   NOT NULL,
    status           varchar(10)   NOT NULL DEFAULT 'PENDING',
    attempt          smallint      NOT NULL DEFAULT 0,
    response_status  smallint,
    response_ms      integer,
    next_retry_at    timestamptz,
    last_error       varchar(500),
    created_at       timestamptz   NOT NULL DEFAULT now(),
    delivered_at     timestamptz,
    CONSTRAINT pk_webhook_deliveries PRIMARY KEY (id),
    CONSTRAINT uq_webhook_deliveries_webhook_event UNIQUE (webhook_id, event_id),
    CONSTRAINT ck_webhook_deliveries_status CHECK (status IN ('PENDING','SUCCEEDED','FAILED','DEAD')),
    CONSTRAINT ck_webhook_deliveries_attempt CHECK (attempt BETWEEN 0 AND 6)
);
CREATE INDEX ix_webhook_deliveries_org_created ON webhook_deliveries (organization_id, created_at DESC, id);
CREATE INDEX ix_webhook_deliveries_retry ON webhook_deliveries (next_retry_at) WHERE status IN ('PENDING','FAILED');
COMMENT ON TABLE webhook_deliveries IS '보내는 Webhook 발송 기록. 최대 6회 재시도 후 DEAD';
COMMENT ON COLUMN webhook_deliveries.webhook_id IS 'data2flow_core.outgoing_webhooks.id (스키마 간 FK 없음)';

-- -----------------------------------------------------------------------------
-- 표준 테이블: 아웃박스 (README §11.1, ADR-020 — 이벤트 무손실)
-- 명령 상태(command.status.*), 기기 상태(device.state.changed), 알림 발송 결과(notification.delivered)를
-- 상태 변경과 같은 트랜잭션에 기록하고, 릴레이가 data2flow.events로 보낸다.
-- -----------------------------------------------------------------------------
CREATE TABLE outboxes (
    id              bigint GENERATED ALWAYS AS IDENTITY,
    organization_id bigint        NOT NULL,
    idempotency_key char(64)      NOT NULL,
    kind            varchar(12)   NOT NULL,
    exchange        varchar(64)   NOT NULL DEFAULT 'data2flow.events',
    routing_key     varchar(128)  NOT NULL,
    payload         jsonb         NOT NULL,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    sent_at         timestamptz,
    attempts        smallint      NOT NULL DEFAULT 0,
    last_error      varchar(500),
    env             varchar(8)    NOT NULL DEFAULT 'prod',
    CONSTRAINT pk_outboxes PRIMARY KEY (id),
    CONSTRAINT uq_outboxes_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_outboxes_kind CHECK (kind IN ('EVENT', 'CALLBACK'))
);
CREATE INDEX ix_outboxes_unsent ON outboxes (env, created_at) WHERE sent_at IS NULL;
COMMENT ON COLUMN outboxes.env IS '행을 만든 배포(dev·stg·prod). 릴레이는 자기 배포 행만 자기 vhost로 보낸다(ADR-030: staging·prod DB 공유)';
CREATE INDEX ix_outboxes_sent_at ON outboxes (sent_at) WHERE sent_at IS NOT NULL;
COMMENT ON TABLE outboxes IS 'action 아웃박스. 이벤트와 core-api 내부 콜백(예: API-OCC-42 구독 만료 알림)을 기록. 보낸 행 7일 보관(2026-10-03 기본값)';

CREATE TABLE processed_messages (
    consumer     varchar(64)  NOT NULL,
    message_id   varchar(64)  NOT NULL,
    processed_at timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT pk_processed_messages PRIMARY KEY (consumer, message_id)
);
CREATE INDEX ix_processed_messages_processed_at ON processed_messages (processed_at);
COMMENT ON TABLE processed_messages IS 'action.commands·action.notify 큐 소비 중복 판정(명령은 commands.idempotency_key로도 막음). 7일 보관(2026-10-03 기본값)';
