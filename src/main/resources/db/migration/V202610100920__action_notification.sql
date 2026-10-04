-- 알림 공통 계층(M4: RUL-02.07·03.01~03.06·05.02·05.03, OPS-06.01·06.03~06.06, ADR-033·048). expand(추가만, ADR-030)
-- 정본: data2flow-docs design/erd/action.md §4.1. 이 파일이 더하는 것:
--   notification_deliveries: 다시 보내기·묶음·에스컬레이션에 필요한 열(요청 키, 사건, 심각도, 수신자 키, 사용자, 보낼 메시지, 단계, 배포)
--   skip_reason 값 확장(VIRTUAL_MUTED·SEVERITY·NOT_LINKED·NO_PERMISSION), attempt 0~6(첫 시도 + 재시도 5회, BR-RUL-17·BR-OPS-06)
--   notification_aggregates(묶음 발송 BR-RUL-15·BR-OPS-07, 방해 금지 끝 요약 OPS-06.05), notification_escalations(BR-RUL-16)

ALTER TABLE notification_deliveries
    ADD COLUMN request_key    char(64),
    ADD COLUMN event          varchar(40),
    ADD COLUMN severity       varchar(10),
    ADD COLUMN recipient_key  varchar(160),
    ADD COLUMN user_id        bigint,
    ADD COLUMN payload        jsonb,
    ADD COLUMN step_no        smallint,
    ADD COLUMN env            varchar(8) NOT NULL DEFAULT 'prod';
COMMENT ON COLUMN notification_deliveries.payload IS '보낼 공통 메시지(주소·제목·본문·링크·버튼·언어). 비밀값 없음. 재시도·다시 보내기에 쓴다';
COMMENT ON COLUMN notification_deliveries.attempt IS '채널 호출 횟수(첫 시도 포함). 최대 6 = 첫 시도 + 재시도 5회(30초·2분·10분·30분·1시간)';
COMMENT ON COLUMN notification_deliveries.next_retry_at IS 'PENDING·RETRYING이면 다음 호출 시각. 묶음·방해 금지로 대기 중이면 NULL(aggregate_id로 묶음 작업이 처리)';
COMMENT ON COLUMN notification_deliveries.env IS '행을 만든 배포(dev·stg·prod). 재시도·묶음 작업은 자기 배포 행만 본다(ADR-030)';

ALTER TABLE notification_deliveries DROP CONSTRAINT ck_notification_deliveries_skip_reason;
ALTER TABLE notification_deliveries ADD CONSTRAINT ck_notification_deliveries_skip_reason CHECK (skip_reason IS NULL OR skip_reason IN
    ('SILENCED','DND','FLAPPING','SUPPRESSED','RENOTIFY_INTERVAL','VIRTUAL_MUTED','SEVERITY','NOT_LINKED','NO_PERMISSION'));
ALTER TABLE notification_deliveries DROP CONSTRAINT ck_notification_deliveries_attempt;
ALTER TABLE notification_deliveries ADD CONSTRAINT ck_notification_deliveries_attempt CHECK (attempt BETWEEN 0 AND 6);

-- 재알림 간격 판정(BR-RUL-13): 같은 알람·수신자·채널의 마지막 SENT
CREATE INDEX ix_notification_deliveries_alarm_recipient ON notification_deliveries (alarm_id, recipient_key, channel_type, sent_at DESC)
    WHERE alarm_id IS NOT NULL AND status = 'SENT';
-- 채널 분당 한도(OPS-06.04)
CREATE INDEX ix_notification_deliveries_channel_sent ON notification_deliveries (channel_id, sent_at) WHERE status = 'SENT';
CREATE INDEX ix_notification_deliveries_aggregate ON notification_deliveries (aggregate_id) WHERE aggregate_id IS NOT NULL;

CREATE TABLE notification_aggregates (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    channel_type     varchar(20)   NOT NULL,
    channel_id       bigint        NOT NULL,
    recipient_key    varchar(160)  NOT NULL,
    aggregate_key    varchar(200)  NOT NULL,
    kind             varchar(10)   NOT NULL,
    window_start     timestamptz   NOT NULL,
    window_end       timestamptz   NOT NULL,
    item_count       integer       NOT NULL DEFAULT 0,
    sent_count       integer       NOT NULL DEFAULT 0,
    status           varchar(10)   NOT NULL DEFAULT 'OPEN',
    summary_delivery_id uuid,
    env              varchar(8)    NOT NULL DEFAULT 'prod',
    created_at       timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_notification_aggregates PRIMARY KEY (id),
    CONSTRAINT ck_notification_aggregates_kind CHECK (kind IN ('EXPLICIT','CHANNEL','DND')),
    CONSTRAINT ck_notification_aggregates_status CHECK (status IN ('OPEN','FLUSHED'))
);
CREATE UNIQUE INDEX uq_notification_aggregates_open ON notification_aggregates (organization_id, channel_id, recipient_key, aggregate_key, kind)
    WHERE status = 'OPEN';
CREATE INDEX ix_notification_aggregates_due ON notification_aggregates (env, window_end) WHERE status = 'OPEN';
COMMENT ON TABLE notification_aggregates IS '묶음 발송 창. EXPLICIT=정책·요청 묶기 창(BR-RUL-15), CHANNEL=채널 기본 묶음·한도(BR-OPS-07), DND=방해 금지 끝 요약(OPS-06.05)';

CREATE TABLE notification_escalations (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    alarm_id         bigint        NOT NULL,
    policy_id        bigint        NOT NULL,
    next_step        smallint      NOT NULL,
    due_at           timestamptz   NOT NULL,
    status           varchar(10)   NOT NULL DEFAULT 'PENDING',
    request          jsonb         NOT NULL,
    env              varchar(8)    NOT NULL DEFAULT 'prod',
    created_at       timestamptz   NOT NULL DEFAULT now(),
    updated_at       timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_notification_escalations PRIMARY KEY (id),
    CONSTRAINT uq_notification_escalations_alarm_policy UNIQUE (alarm_id, policy_id),
    CONSTRAINT ck_notification_escalations_status CHECK (status IN ('PENDING','DONE','CANCELLED')),
    CONSTRAINT ck_notification_escalations_step CHECK (next_step BETWEEN 2 AND 3)
);
CREATE INDEX ix_notification_escalations_due ON notification_escalations (env, due_at) WHERE status = 'PENDING';
COMMENT ON TABLE notification_escalations IS '에스컬레이션 대기(BR-RUL-16, 최대 3단계). 확인·해제(alarm.acked·cleared)면 CANCELLED';
COMMENT ON COLUMN notification_escalations.request IS '처음 알림 요청(NotificationRequest). 다음 단계 알림의 바탕';
