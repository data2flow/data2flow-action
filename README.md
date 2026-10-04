# data2flow-action

행동 실행: 제어 창구(Facade)·드라이버·명령 추적, 알림 발송(텔레그램, 채널 SPI), Sink·보내는 Webhook.

- 관련 스펙: ACT, RUL-03, FLW-04, OPS-06/09 (정본은 비공개 저장소 `data2flow-docs`)
- 패키지: `net.java21.data2flow.action` · Spring Boot 4.1.1 · Java 21 · Maven Wrapper
- 포트: API 8080, actuator 8081(프로브·지표 전용)

## M3 기능 (가상 폐루프)

화면·플로우·AI의 모든 제어 명령은 제어 창구 하나를 지나 드라이버로 갑니다(ADR-009, ACT-02.01).

| 영역 | 내용 | 스펙 |
|---|---|---|
| 제어 창구 | 멱등 → 권한(DEVICE_CONTROL) → 샌드박스 → 기능 스키마 → 모델 제약 → 조직 절대 한계 → 수동 우선 → 변경 없음 → 보호(지연·차단) → 진동·최소 간격 → 오프라인 대기 → 드라이버. 거부도 기록 | ACT-01.03, 02.01, 02.05, 06.01, 06.04 |
| 명령 추적 | REQUESTED → SENT → ACKED → APPLIED, TIMEOUT(30초 ack·60초 적용), FAILED, EXPIRED, SUPERSEDED, CANCELLED. 타임라인·EVT-ACT-01 | ACT-02.02·02.03 |
| 상태 쌍 | desired/reported/delta, 버전이 큰 보고만 반영, 수동 우선 30분, 재연결 재적용, 상태 구간 | ACT-02.04, TSD-01.03 |
| 드라이버 | SPI(`actuation.driver`)와 계약 키트 `DriverContractTest`, virtual(시뮬레이터 내부 API), MQTT(기본 꺼짐) | ACT-03.01·03.02 |

### 내부 API (core-api만 부름, ACT-api §5.2)

`POST /internal/action/commands`, `GET /internal/action/commands/{command-id}`, `POST …/{command-id}/cancel`,
`GET /internal/action/devices/{device-id}/commands`(커서 이력)·`/shadow`·`/control`, `DELETE …/{device-id}/manual-override`,
`POST /internal/action/drivers/{driver-id}/healthcheck`

### 메시지

| 방향 | 채널 | 내용 |
|---|---|---|
| 소비 | `data2flow.actions` · `command` → `action.commands`(Quorum, DLX) | `ActionRequest` v1 kind=COMMAND(flow-engine·core 아웃박스). `executed_actions`로 한 번만 실행 |
| 소비 | `data2flow.events` → `action.events` | `device.command.ack`, `device.state.reported`(EVT-SIM-03), `device.connectivity.changed`, `device.changed` |
| 소비 | `data2flow.config` → 임시 큐 | 캐시 무효화: DEVICE(그 기기)·MODEL(그 모델)·DRIVER(연결된 기기)·CAPABILITY(그 기능을 쓰는 기기)·SPACE·SETTING(전체)·SIM_SANDBOX(샌드박스 목록) |
| 발행(아웃박스) | `data2flow.events` | `command.status.{status}`(EVT-ACT-01), `device.state.changed`(EVT-ACT-02) |
| 호출 | core 내부 API | 제어 프로필(API-ACT-40), 샌드박스 공간(API-ACT-41), 관계 대상(API-DEV-128), 권한 판정, 감사(API-IAM-39, 아웃박스) |
| 호출 | simulator 내부 API | `POST /internal/sim/devices/{device-id}/commands`, `GET …/state`(API-SIM-30·32) |

### 안전장치

- MQTT 드라이버는 `data2flow.action.mqtt.enabled=true`일 때만 만들어지고(기본 꺼짐), 공용 브로커 `iot-data.java21.net`(하위 이름·같은 IP 포함)은 생성할 때 거부합니다(CLAUDE.md §5, ⏸ ACT-03.02 결정 대기). 시험은 Testcontainers Mosquitto만 씁니다.
- 샌드박스 판정(BR-ACT-23)의 출처 공간은 행동 요청의 `source.spaceId`(플로우가 적음), 없으면 관계 대상 `target.spaceId`입니다. 기기 대상 플로우 명령도 샌드박스 공간 출처면 실제 기기를 거부합니다.
- 로컬(local 프로필)은 큐 소비·기한 작업·아웃박스 릴레이를 끕니다. 운영과 같은 장비에 제어가 한 번 더 나가지 않게 하려는 것입니다(deployment.md §8.2).
- staging과 prod는 DB를 함께 쓰므로 명령·아웃박스 행에 배포(`env`)를 적고, 각 배포는 자기 행만 처리합니다(ADR-030·043).

### 설정

| 키 | 기본값 | 설명 |
|---|---|---|
| `data2flow.action.core-uri` / `simulator-uri` | `http://data2flow-core-api` / `http://data2flow-simulator` | 내부 주소(`DATA2FLOW_CORE_URI`·`DATA2FLOW_SIMULATOR_URI`) |
| `data2flow.action.env` | 프로필별 dev·stg·prod | 배포 구분 |
| `data2flow.action.command.*` | ack 30s, apply 60s, 재시도 3회(1s·2배·최대 10s), wait 10s, 유효 600s | 드라이버 설정이 없을 때 기본값 |
| `data2flow.action.mqtt.enabled` | `false` | MQTT 드라이버 |
| `data2flow.action.messaging.enabled` / `scheduler.enabled` / `outbox.relay-enabled` | `true` (local은 `false`) | 큐 소비·기한 작업·릴레이 |

## 빌드와 실행

```bash
./mvnw verify                 # 단위·계약(Mosquitto)·통합(Testcontainers PostgreSQL 18·RabbitMQ 3.13) + 커버리지 80% 검사(Docker 필요)
./mvnw spring-boot:run        # 로컬 실행(프로필 local)
```

공통 라이브러리 `data2flow-contracts`는 GitHub Packages에 있어서 읽기에도 토큰이 필요합니다. `~/.m2/settings.xml`에 서버 `github`(사용자 이름 + `read:packages` 권한 토큰)를 넣거나, `data2flow-contracts`를 받아 `./mvnw install`로 로컬 저장소에 설치합니다.

## 작업 규칙

스펙 ID에서 시작하고(인수 테스트 → 테스트 케이스 → 구현), 브랜치·PR·테스트 이름에 스펙 ID를 남깁니다. 1.0 전에는 `main` + `feat/<스펙ID>-<요약>`, 1.0 뒤에는 버전 브랜치 `feature/vX.Y`를 씁니다(ADR-039).
