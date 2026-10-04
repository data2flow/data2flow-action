-- action M4 제어(actuation) 확장: 드라이버 지표·서킷 브레이커(ACT-03.06·07.03, BR-ACT-14), 제어 효과 확인(ACT-08.01, BR-ACT-20),
-- LoRaWAN Class A 다운링크 대기 기한(ACT-07.02). expand만(새 테이블·인덱스): staging과 prod가 DB를 함께 쓴다(ADR-030).
-- 정본: data2flow-docs design/erd/action.md §2·§3(ADR-049)

-- -----------------------------------------------------------------------------
-- 드라이버 호출 기록(지표 1시간·24시간 창, 서킷 실패율 1분 창). 2일 보관(주기 작업이 지운다)
-- -----------------------------------------------------------------------------
CREATE TABLE driver_calls (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    organization_id  bigint        NOT NULL,
    driver_id        bigint        NOT NULL,
    driver_type      varchar(20)   NOT NULL,
    at               timestamptz   NOT NULL,
    ok               boolean       NOT NULL,
    latency_ms       integer       NOT NULL,
    command_id       uuid,
    error            varchar(500),
    CONSTRAINT pk_driver_calls PRIMARY KEY (id)
);
CREATE INDEX ix_driver_calls_driver_at ON driver_calls (driver_id, at DESC);
CREATE INDEX ix_driver_calls_at ON driver_calls (at);
COMMENT ON TABLE driver_calls IS '드라이버 호출 한 번(ACT-03.06 지표, BR-ACT-14 서킷 실패율). 2일 보관';
COMMENT ON COLUMN driver_calls.driver_id IS 'data2flow_core.drivers.id (스키마 간 FK 없음)';

-- -----------------------------------------------------------------------------
-- 드라이버 서킷 상태(파드 사이 공유). 열림 30초 동안 즉시 FAILED(DRIVER_UNAVAILABLE)
-- -----------------------------------------------------------------------------
CREATE TABLE driver_circuits (
    driver_id         bigint        NOT NULL,
    organization_id   bigint        NOT NULL,
    driver_type       varchar(20)   NOT NULL,
    state             varchar(10)   NOT NULL DEFAULT 'CLOSED',
    opened_at         timestamptz,
    failure_rate      double precision,
    trial_started_at  timestamptz,
    updated_at        timestamptz   NOT NULL DEFAULT now(),
    CONSTRAINT pk_driver_circuits PRIMARY KEY (driver_id),
    CONSTRAINT ck_driver_circuits_state CHECK (state IN ('CLOSED','OPEN','HALF_OPEN'))
);
COMMENT ON TABLE driver_circuits IS '드라이버 서킷 브레이커 상태(BR-ACT-14: 1분 실패율 50% 초과 → 30초 OPEN → HALF_OPEN 시험 호출)';

-- -----------------------------------------------------------------------------
-- 제어 효과 확인 대기(ACT-08.01): APPLIED 시각 + 기대 시간 뒤에 측정값 방향을 본다
-- -----------------------------------------------------------------------------
CREATE TABLE effect_checks (
    command_id       uuid              NOT NULL,
    organization_id  bigint            NOT NULL,
    device_id        bigint            NOT NULL,
    space_id         bigint,
    capability       varchar(64)       NOT NULL,
    metric           varchar(64)       NOT NULL,
    direction        varchar(4)        NOT NULL,
    within_minutes   integer           NOT NULL,
    start_at         timestamptz       NOT NULL,
    due_at           timestamptz       NOT NULL,
    status           varchar(12)       NOT NULL DEFAULT 'PENDING',
    start_value      double precision,
    end_value        double precision,
    event_emitted    boolean           NOT NULL DEFAULT false,
    finished_at      timestamptz,
    env              varchar(8)        NOT NULL DEFAULT 'prod',
    CONSTRAINT pk_effect_checks PRIMARY KEY (command_id),
    CONSTRAINT ck_effect_checks_direction CHECK (direction IN ('UP','DOWN')),
    CONSTRAINT ck_effect_checks_status CHECK (status IN ('PENDING','EFFECTIVE','NO_EFFECT','UNKNOWN'))
);
CREATE INDEX ix_effect_checks_due ON effect_checks (env, due_at) WHERE status = 'PENDING';
CREATE INDEX ix_effect_checks_device ON effect_checks (device_id, finished_at DESC) WHERE status = 'NO_EFFECT';
COMMENT ON TABLE effect_checks IS '제어 효과 확인(BR-ACT-20). NO_EFFECT면 EVT-ACT-04(같은 기기 1시간에 1회)';

-- LoRaWAN Class A 다운링크 대기(QUEUED_FOR_DOWNLINK)의 유효 기한도 기한 작업이 본다(ACT-07.02)
CREATE INDEX ix_commands_downlink ON commands (device_id) WHERE status = 'QUEUED_FOR_DOWNLINK';
CREATE INDEX ix_commands_downlink_timeout ON commands (timeout_at) WHERE status = 'QUEUED_FOR_DOWNLINK';
