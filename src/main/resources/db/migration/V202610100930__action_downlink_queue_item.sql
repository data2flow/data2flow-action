-- LoRaWAN 다운링크 결과(EVT-ACT-09 lorawan.downlink.ack)로 명령을 찾는 열(ACT-03.03, ADR-054 남은 것 ①).
-- 드라이버가 ChirpStack 큐에 등록한 항목 ID를 명령에 남긴다(메모리 대신 DB: 파드가 바뀌거나 재시작해도 ack를 이어받는다).
-- expand만(열·인덱스 추가, 이미 있는 값 채우기): staging과 prod가 DB를 함께 쓴다(ADR-030). 정본: data2flow-docs design/erd/action.md
ALTER TABLE commands ADD COLUMN downlink_queue_item_id varchar(64);
COMMENT ON COLUMN commands.downlink_queue_item_id IS 'ChirpStack 다운링크 큐 항목 ID(LoRaWAN 드라이버 등록 응답 id). EVT-ACT-09로 명령을 찾는다';

UPDATE commands SET downlink_queue_item_id = driver_response ->> 'queueItemId'
 WHERE driver_response ? 'queueItemId' AND downlink_queue_item_id IS NULL;

CREATE INDEX ix_commands_downlink_queue_item ON commands (organization_id, downlink_queue_item_id)
    WHERE downlink_queue_item_id IS NOT NULL;
