package net.java21.data2flow.action.common;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;

/**
 * data2flow-action 설정({@code data2flow.action.*}). 비밀값은 넣지 않는다(환경변수·k8s Secret).
 *
 * @param env           배포 환경 표시(dev·stg·prod·test). 명령·아웃박스 행에 남겨 staging과 prod가 DB 하나를 함께 써도(ADR-030)
 *                      각 배포가 자기 행만 릴레이·기한 처리하게 한다
 * @param instanceId    파드 이름(MQTT client-id 순번 등)
 * @param flywayMode    {@code migrate}(staging·시험) 또는 {@code validate}(prod·local)
 * @param coreUri       core-api 내부 주소(제어 프로필·샌드박스·권한·감사)
 * @param simulatorUri  simulator 내부 주소(virtual 드라이버, API-SIM-30·32)
 * @param profileTtl    제어 프로필 캐시 최대 수명(설정 변경 메시지를 놓쳐도 이 시간 안에 다시 읽는다)
 * @param scheduler     명령 기한·재시도·지연 실행 작업
 * @param outbox        아웃박스 릴레이
 * @param command       명령 기본값
 * @param mqtt          MQTT 일반 드라이버(기본 꺼짐, 공용 브로커 금지)
 * @param effect        제어 효과 확인·가동 집계(ACT-08)
 * @param lorawan       LoRaWAN(ChirpStack) 드라이버(기본 꺼짐, 공용 ChirpStack 금지)
 * @param vendors       클라우드 벤더 드라이버(LG ThinQ·SmartThings, 기본 꺼짐 — 키 발급 전 "준비 중", ADR-040)
 */
@ConfigurationProperties(prefix = "data2flow.action")
public record ActionProperties(String env, String instanceId, String flywayMode, String coreUri, String simulatorUri,
                               Duration profileTtl, Scheduler scheduler, Outbox outbox, CommandDefaults command, Mqtt mqtt,
                               Effect effect, LoRaWan lorawan, Vendors vendors) {

    @org.springframework.boot.context.properties.bind.ConstructorBinding
    public ActionProperties {
        env = blank(env) ? "dev" : env;
        instanceId = blank(instanceId) ? "action-local-0" : instanceId;
        flywayMode = blank(flywayMode) ? "validate" : flywayMode;
        coreUri = blank(coreUri) ? "http://data2flow-core-api" : coreUri;
        simulatorUri = blank(simulatorUri) ? "http://data2flow-simulator" : simulatorUri;
        profileTtl = profileTtl == null ? Duration.ofSeconds(30) : profileTtl;
        scheduler = scheduler == null ? new Scheduler(true, Duration.ofSeconds(1), 100) : scheduler;
        outbox = outbox == null ? new Outbox(true, Duration.ofSeconds(1), 100, Duration.ofDays(7)) : outbox;
        command = command == null ? new CommandDefaults(null, null, null, null, null, null, null, null) : command;
        mqtt = mqtt == null ? new Mqtt(false, null, 1883, null, null, null, null, 1) : mqtt;
        effect = effect == null ? new Effect(null, null) : effect;
        lorawan = lorawan == null ? new LoRaWan(false, null, null) : lorawan;
        vendors = vendors == null ? new Vendors(null, null) : vendors;
    }

    /** M3 모양(효과·LoRaWAN·벤더 기본값) */
    public ActionProperties(String env, String instanceId, String flywayMode, String coreUri, String simulatorUri, Duration profileTtl,
                            Scheduler scheduler, Outbox outbox, CommandDefaults command, Mqtt mqtt) {
        this(env, instanceId, flywayMode, coreUri, simulatorUri, profileTtl, scheduler, outbox, command, mqtt, null, null, null);
    }

    /**
     * 제어 효과 확인(BR-ACT-20)과 가동 집계(BR-ACT-21).
     *
     * @param minChange 기대 방향 최소 변화(측정 단위, 기본 0.2 — 온도면 0.2℃)
     * @param zone      일 집계 날짜 경계(사이트 시간대, 기본 Asia/Seoul)
     */
    public record Effect(Double minChange, java.time.ZoneId zone) {
        public Effect {
            minChange = minChange == null || minChange < 0 ? 0.2 : minChange;
            zone = zone == null ? java.time.ZoneId.of("Asia/Seoul") : zone;
        }
    }

    /**
     * LoRaWAN 다운링크 드라이버(ACT-03.03). ⏸ 결정 대기: 공용 ChirpStack(s3)에 다운링크를 넣지 않는다(CLAUDE.md §5, ADR-029).
     * 기본 꺼짐이고, 켜도 {@code deniedHosts}와 항상 금지하는 공용 주소는 거부한다. 지금은 ChirpStack API 목(MockWebServer)으로만 시험한다.
     *
     * @param enabled     켜기(기본 false)
     * @param deniedHosts 추가 금지 주소
     * @param timeout     ChirpStack API 호출 제한 시간(기본 5초)
     */
    public record LoRaWan(boolean enabled, List<String> deniedHosts, Duration timeout) {
        /** 공용 인프라. 설정으로 지울 수 없게 항상 금지한다(s3 ChirpStack, 공용 MQTT 브로커) */
        public static final List<String> SHARED_HOSTS = List.of("s3.java21.net", "iot-data.java21.net");

        public LoRaWan {
            deniedHosts = deniedHosts == null ? List.of() : List.copyOf(deniedHosts);
            timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
        }
    }

    /**
     * 클라우드 벤더 드라이버(ACT-03.04, ADR-040). 키가 없어 기본 꺼짐("준비 중"). 켜면 {@code baseUrl}의 벤더 API를 부른다.
     *
     * @param lgThinq     LG ThinQ Connect
     * @param smartThings SmartThings
     */
    public record Vendors(Vendor lgThinq, Vendor smartThings) {
        public Vendors {
            lgThinq = lgThinq == null ? new Vendor(false, "https://api-kic.lgthinq.com", null) : lgThinq;
            smartThings = smartThings == null ? new Vendor(false, "https://api.smartthings.com", null) : smartThings;
        }
    }

    /**
     * @param enabled 켜기(기본 false — 키 발급 전)
     * @param baseUrl API 주소
     * @param timeout 호출 제한 시간(기본 5초)
     */
    public record Vendor(boolean enabled, String baseUrl, Duration timeout) {
        public Vendor {
            timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
        }
    }

    static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    /**
     * @param enabled 주기 실행(시험은 끄고 직접 부른다)
     * @param period  주기
     * @param batch   한 번에 처리할 최대 행 수
     */
    public record Scheduler(boolean enabled, Duration period, int batch) {
        public Scheduler {
            period = period == null ? Duration.ofSeconds(1) : period;
            batch = batch <= 0 ? 100 : batch;
        }
    }

    /**
     * @param relayEnabled 주기 릴레이(시험은 끄고 직접 부른다)
     * @param interval     릴레이 주기
     * @param batchSize    한 번에 보낼 행 수
     * @param retention    보낸 행 보관(ERD README §14, 7일)
     */
    public record Outbox(boolean relayEnabled, Duration interval, int batchSize, Duration retention) {
        public Outbox {
            interval = interval == null ? Duration.ofSeconds(1) : interval;
            batchSize = batchSize <= 0 ? 100 : batchSize;
            retention = retention == null ? Duration.ofDays(7) : retention;
        }
    }

    /**
     * 명령 기본값(드라이버 설정이 없을 때, ACT domain-model drivers 기본값).
     *
     * @param ackTimeout       ack 기한(기본 30초, ACT-02.02)
     * @param applyTimeout     적용 기한(기본 60초)
     * @param retryMaxAttempts 드라이버 호출 최대 횟수(BR-ACT-14, 기본 3)
     * @param retryInitial     첫 재시도 간격(1초)
     * @param retryMultiplier  배수(2)
     * @param retryMax         최대 간격(10초)
     * @param waitMax          {@code wait=ack|applied} 최대 대기(10초, API-ACT-01)
     * @param defaultValidity  유효 시간 기본값(600초, 조직 설정이 없을 때)
     */
    public record CommandDefaults(Duration ackTimeout, Duration applyTimeout, Integer retryMaxAttempts, Duration retryInitial,
                                  Double retryMultiplier, Duration retryMax, Duration waitMax, Duration defaultValidity) {
        public CommandDefaults {
            ackTimeout = ackTimeout == null ? Duration.ofSeconds(30) : ackTimeout;
            applyTimeout = applyTimeout == null ? Duration.ofSeconds(60) : applyTimeout;
            retryMaxAttempts = retryMaxAttempts == null || retryMaxAttempts < 1 ? 3 : retryMaxAttempts;
            retryInitial = retryInitial == null ? Duration.ofSeconds(1) : retryInitial;
            retryMultiplier = retryMultiplier == null || retryMultiplier < 1 ? 2.0 : retryMultiplier;
            retryMax = retryMax == null ? Duration.ofSeconds(10) : retryMax;
            waitMax = waitMax == null ? Duration.ofSeconds(10) : waitMax;
            defaultValidity = defaultValidity == null ? Duration.ofSeconds(600) : defaultValidity;
        }
    }

    /**
     * MQTT 일반 드라이버(ACT-03.02). ⏸ 결정 대기: 공용 플랫폼 브로커({@code iot-data.java21.net})에 명령을 발행하지 않는다
     * (CLAUDE.md §5). 그래서 기본 꺼짐이고, 켜더라도 {@code deniedHosts}에 있는 주소는 접속을 거부한다. 지금은 Testcontainers
     * Mosquitto로만 쓴다.
     *
     * @param enabled     켜기(기본 false)
     * @param host        브로커 주소
     * @param port        포트(1883)
     * @param username    사용자(선택)
     * @param password    비밀번호(선택, 환경변수)
     * @param clientIdBase client-id 앞부분(기본 data2flow-action). 실제 ID는 contracts {@code ClientIds}
     * @param deniedHosts 접속 금지 주소(기본 공용 브로커 iot-data.java21.net)
     * @param qos         명령 QoS(1)
     */
    public record Mqtt(boolean enabled, String host, int port, String username, String password, String clientIdBase,
                       List<String> deniedHosts, int qos) {
        /** 공용 플랫폼 브로커. 설정으로 지울 수 없게 항상 금지 목록에 더한다 */
        public static final String SHARED_PLATFORM_BROKER = "iot-data.java21.net";

        public Mqtt {
            port = port <= 0 ? 1883 : port;
            clientIdBase = blank(clientIdBase) ? "data2flow-action" : clientIdBase;
            deniedHosts = deniedHosts == null ? List.of() : List.copyOf(deniedHosts);
            qos = qos < 0 || qos > 2 ? 1 : qos;
        }
    }
}
