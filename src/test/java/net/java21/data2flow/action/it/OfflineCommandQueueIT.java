package net.java21.data2flow.action.it;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.DriverBinding;
import net.java21.data2flow.action.actuation.domain.ModelCapability;
import net.java21.data2flow.action.support.FakeSimulator;
import net.java21.data2flow.action.support.Fixtures;
import net.java21.data2flow.action.support.IntegrationTestSupport;
import net.java21.data2flow.contracts.message.EventType;
import net.java21.data2flow.contracts.message.event.DeviceConnectivityChanged;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 오프라인 대기열(ACT-07.01)·LoRaWAN Class A(ACT-07.02)·드라이버 재시도와 서킷 브레이커(ACT-07.03). TC-ACT-122·123·126·129
 */
class OfflineCommandQueueIT extends IntegrationTestSupport {

    private static final long VALVE = 31_001;   // Class A 밸브(Switch)
    private RestClient operator;
    @org.springframework.beans.factory.annotation.Autowired
    private io.micrometer.core.instrument.MeterRegistry meters;

    @BeforeEach
    void setUp() {
        CORE.profiles.put(Fixtures.AIRCON, Fixtures.virtualAircon());
        CORE.profiles.put(VALVE, new ControlProfile(VALVE, Fixtures.ORG, Fixtures.SPACE, "Class A 밸브", "70b3d57ed0000001", true, "ACTIVE",
                5L, Map.of("Switch", new ModelCapability(Map.of(), null, false, true)), new DriverBinding(9L, "VIRTUAL", Map.of(), 30, 60, null),
                Fixtures.settings()).withReportInterval(600));
        CORE.roles.put(Fixtures.USER, "OPERATOR");
        operator = api(Fixtures.USER, Fixtures.ORG);
    }

    private void offline(long deviceId) {
        publish(EventType.DEVICE_CONNECTIVITY_CHANGED, new DeviceConnectivityChanged(deviceId, DeviceConnectivityChanged.Connectivity.ONLINE, DeviceConnectivityChanged.Connectivity.OFFLINE, clock.instant(), 300, 3));
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = "
                + deviceId + " AND connectivity = 'OFFLINE'") == 1);
    }

    private String submit(long deviceId, String capability, Map<String, Object> args, String key) {
        Result r = post(operator, "/internal/action/commands", Map.of("deviceId", Long.toString(deviceId), "capability", capability,
                "command", "set", "args", args), key);
        return r.response().path("id").asString();
    }

    @Test
    @DisplayName("[ACT-07.01][AT-ACT-07.1][TC-ACT-122] 오프라인 기기 명령 → QUEUED, 5분 뒤 온라인 → 바로 SENT → APPLIED")
    void queuedThenApplied() {
        offline(Fixtures.AIRCON);
        String id = submit(Fixtures.AIRCON, "Switch", Map.of("on", true), "q-1");
        assertThat(status(id)).isEqualTo("QUEUED");

        clock.advanceBy(Duration.ofMinutes(5));
        publish(EventType.DEVICE_CONNECTIVITY_CHANGED, new DeviceConnectivityChanged(Fixtures.AIRCON, DeviceConnectivityChanged.Connectivity.OFFLINE, DeviceConnectivityChanged.Connectivity.ONLINE, clock.instant(), 300, 3));

        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("APPLIED"));
        assertThat(jdbc.sql("SELECT string_agg(to_status, ',' ORDER BY id) FROM data2flow_action.command_events WHERE command_id = CAST(:id AS uuid)")
                .param("id", id).query(String.class).single()).contains("QUEUED,REQUESTED,SENT");
    }

    @Test
    @DisplayName("[ACT-07.01][AT-ACT-07.2][TC-ACT-123] 오프라인 대기 10분 경과 → FAILED(EXPIRED)와 command.status.failed(요청자 알림)")
    void expired() {
        offline(Fixtures.AIRCON);
        String id = submit(Fixtures.AIRCON, "Switch", Map.of("on", true), "q-2");
        clock.advanceBy(Duration.ofMinutes(10).minusSeconds(1));
        tracker.processDue();
        assertThat(status(id)).isEqualTo("QUEUED");
        clock.advanceBy(Duration.ofSeconds(1));
        tracker.processDue();

        assertThat(status(id)).isEqualTo("FAILED");
        await().atMost(Duration.ofSeconds(20)).until(() -> events("command.status.failed").stream()
                .anyMatch(e -> e.path("payload").path("reason").asString().equals("EXPIRED")));
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-126] Class A(보고 주기 10분) 명령 → QUEUED_FOR_DOWNLINK·예상 시각, 다음 업링크 직후 보내 ACKED 이상")
    void classA() {
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(VALVE, 1, Map.of("Switch", Map.of("on", false)), clock.instant(), true));
        await().atMost(Duration.ofSeconds(20)).until(() -> count(
                "SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = " + VALVE + " AND reported_version = 1") == 1);
        clock.advanceBy(Duration.ofMinutes(4));

        String id = submit(VALVE, "Switch", Map.of("on", true), "a-1");

        Result detail = get(operator, "/internal/action/commands/" + id);
        assertThat(detail.response().path("status").asString()).isEqualTo("QUEUED_FOR_DOWNLINK");
        assertThat(detail.response().path("expectedDeliveryAt").asString()).isEqualTo(clock.instant().plus(Duration.ofMinutes(6)).toString());
        assertThat(SIM.commandsFor(VALVE)).isZero();

        clock.advanceBy(Duration.ofMinutes(6));
        SIM.versions.put(VALVE, 2L);
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(VALVE, 2, Map.of("Switch", Map.of("on", false)), clock.instant(), true));

        await().atMost(Duration.ofSeconds(20)).until(() -> status(id).equals("ACKED") || status(id).equals("APPLIED"));
        assertThat(SIM.commandsFor(VALVE)).isEqualTo(1);
    }

    /** pipeline이 LoRaWAN 업링크(ChirpStack event/up)마다 내는 업링크 신호: 기능 상태 없음, 버전 = fCnt, 실제 기기 */
    private void uplinkSignal(long deviceId, long fCnt) {
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(deviceId, fCnt, Map.of(), clock.instant(), false));
    }

    /** 처리한 업링크 신호 수(지표 data2flow_action_uplink_signals_total) */
    private double signals() {
        return meters.counter("data2flow_action_uplink_signals_total").count();
    }

    private java.time.Instant reportedAt(long deviceId) {
        return jdbc.sql("SELECT reported_at FROM data2flow_action.device_shadows WHERE device_id = " + deviceId)
                .query(java.sql.Timestamp.class).optional().map(java.sql.Timestamp::toInstant).orElse(null);
    }

    @Test
    @DisplayName("[ACT-07.02][AT-ACT-07.5][TC-ACT-126] pipeline 업링크 신호(EVT-ACT-07 상태 없음): 마지막 업링크 시각만 갱신(상태·버전·EVT-ACT-02 없음), 다음 신호 직후 대기 다운링크를 보낸다")
    void classAUplinkSignalFlushesDownlink() {
        double start = signals();
        publish(EventType.DEVICE_STATE_REPORTED, new DeviceStateReported(VALVE, 1, Map.of("Switch", Map.of("on", false)), clock.instant(), true));
        await().atMost(Duration.ofSeconds(20)).until(() -> count(
                "SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = " + VALVE + " AND reported_version = 1") == 1);
        int changedBefore = events("device.state.changed").size();

        clock.advanceBy(Duration.ofMinutes(4));
        java.time.Instant firstUplink = clock.instant();
        uplinkSignal(VALVE, 1042);
        await().atMost(Duration.ofSeconds(20)).until(() -> firstUplink.equals(reportedAt(VALVE)));
        assertThat(count("SELECT reported_version FROM data2flow_action.device_shadows WHERE device_id = " + VALVE)).isEqualTo(1);
        assertThat(jdbc.sql("SELECT reported::text FROM data2flow_action.device_shadows WHERE device_id = " + VALVE).query(String.class).single())
                .contains("\"on\": false");

        clock.advanceBy(Duration.ofMinutes(1));
        String id = submit(VALVE, "Switch", Map.of("on", true), "a-3");
        Result detail = get(operator, "/internal/action/commands/" + id);
        assertThat(detail.response().path("status").asString()).isEqualTo("QUEUED_FOR_DOWNLINK");
        // 예상 시각 = 마지막 업링크(신호) + 보고 주기 10분
        assertThat(detail.response().path("expectedDeliveryAt").asString()).isEqualTo(firstUplink.plus(Duration.ofMinutes(10)).toString());

        clock.advanceBy(Duration.ofMinutes(9));
        uplinkSignal(VALVE, 1043);
        await().atMost(Duration.ofSeconds(20)).until(() -> SIM.commandsFor(VALVE) == 1);
        await().atMost(Duration.ofSeconds(20)).until(() -> !status(id).equals("QUEUED_FOR_DOWNLINK") && !status(id).equals("REQUESTED"));
        assertThat(jdbc.sql("SELECT string_agg(to_status, ',' ORDER BY id) FROM data2flow_action.command_events WHERE command_id = CAST(:id AS uuid)")
                .param("id", id).query(String.class).single()).contains("QUEUED_FOR_DOWNLINK,REQUESTED,SENT");

        // 같은 신호를 다시 받아도(최소 1회 전달) 다시 보내지 않고, 늦게 온 신호는 시각을 되돌리지 않는다
        java.time.Instant secondUplink = clock.instant();
        await().atMost(Duration.ofSeconds(20)).until(() -> signals() >= start + 2);
        double handled = start + 2;
        uplinkSignal(VALVE, 1043);
        clock.advanceBy(Duration.ofSeconds(-30));
        uplinkSignal(VALVE, 1041);
        clock.advanceBy(Duration.ofSeconds(30));
        await().atMost(Duration.ofSeconds(20)).until(() -> signals() >= handled + 2);
        assertThat(reportedAt(VALVE)).isEqualTo(secondUplink);
        long changedBySignals = events("device.state.changed").stream()
                .filter(e -> e.path("payload").path("reportedVersion").asLong() > 1000).count();
        assertThat(changedBySignals).isZero();
        assertThat(events("device.state.changed").size()).isGreaterThanOrEqualTo(changedBefore);
        assertThat(SIM.commandsFor(VALVE)).isEqualTo(1);
    }

    @Test
    @DisplayName("[ACT-07.02][TC-ACT-128] 제어한 적 없는 LoRaWAN 센서의 업링크 신호는 상태 쌍을 만들지 않는다(센서마다 행·이벤트 없음)")
    void uplinkSignalOfSensorIsNoOp() {
        long sensor = 31_002;
        double handled = signals();
        uplinkSignal(sensor, 7);
        await().atMost(Duration.ofSeconds(20)).until(() -> signals() >= handled + 1);
        assertThat(count("SELECT count(*) FROM data2flow_action.device_shadows WHERE device_id = " + sensor)).isZero();
        assertThat(events("device.state.changed")).isEmpty();
    }

    @Test
    @DisplayName("[ACT-07.02][TC-ACT-128] Class A 대기 명령은 유효 시간이 지나면 FAILED(EXPIRED)")
    void classAExpired() {
        String id = submit(VALVE, "Switch", Map.of("on", true), "a-2");
        assertThat(status(id)).isEqualTo("QUEUED_FOR_DOWNLINK");
        clock.advanceBy(Duration.ofMinutes(10).plusSeconds(1));
        tracker.processDue();
        assertThat(status(id)).isEqualTo("FAILED");
    }

    @Test
    @DisplayName("[ACT-07.03][AT-ACT-07.4][TC-ACT-129] 드라이버 대상 장애(503)에서 명령 3건 → 백오프 재시도 뒤 서킷 열림, 이후 명령 즉시 FAILED(DRIVER_UNAVAILABLE), 열림 이벤트 1건")
    void circuitOpens() {
        // TC-ACT-129는 MQTT 브로커 중단이지만 서킷은 드라이버 종류와 무관한 창구 단계라 virtual 드라이버(시뮬레이터 503)로 같은 경로를 시험한다
        listen("driver.circuit.#");
        SIM.modes.put(Fixtures.AIRCON, FakeSimulator.Mode.ERROR);
        String c1 = submit(Fixtures.AIRCON, "Switch", Map.of("on", true), "c-1");
        String c2 = submit(Fixtures.AIRCON, "Thermostat", Map.of("targetTemperature", 24), "c-2");
        String c3 = submit(Fixtures.AIRCON, "Thermostat", Map.of("mode", "fan"), "c-3");
        assertThat(status(c1)).isEqualTo("REQUESTED");   // 재시도 대기(1초 뒤)
        clock.advanceBy(Duration.ofSeconds(1));
        tracker.processDue();
        clock.advanceBy(Duration.ofSeconds(2));
        tracker.processDue();

        await().atMost(Duration.ofSeconds(20)).until(() -> count(
                "SELECT count(*) FROM data2flow_action.driver_circuits WHERE driver_id = 9 AND state = 'OPEN'") == 1);
        for (String c : new String[]{c1, c2, c3}) {
            assertThat(status(c)).isEqualTo("FAILED");
        }
        assertThat(jdbc.sql("SELECT status_reason FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)").param("id", c3)
                .query(String.class).single()).isIn("DRIVER_UNAVAILABLE", "DRIVER_ERROR");

        long before = SIM.commandsFor(Fixtures.AIRCON);
        String c4 = submit(Fixtures.AIRCON, "Switch", Map.of("on", false), "c-4");
        assertThat(status(c4)).isEqualTo("FAILED");
        assertThat(jdbc.sql("SELECT status_reason FROM data2flow_action.commands WHERE id = CAST(:id AS uuid)").param("id", c4)
                .query(String.class).single()).isEqualTo("DRIVER_UNAVAILABLE");
        assertThat(SIM.commandsFor(Fixtures.AIRCON)).isEqualTo(before);   // 서킷이 열려 드라이버를 부르지 않음
        await().atMost(Duration.ofSeconds(20)).until(() -> events("driver.circuit.opened").size() == 1);

        // 30초 뒤 시험 호출이 성공하면 닫힌다
        SIM.modes.put(Fixtures.AIRCON, FakeSimulator.Mode.RESPOND);
        clock.advanceBy(Duration.ofSeconds(30));
        // (Switch on을 다시 내면 60초 안 on→off→on 진동 차단에 걸리므로 다른 기능으로 시험 호출)
        String c5 = submit(Fixtures.AIRCON, "Thermostat", Map.of("mode", "cool"), "c-5");
        await().atMost(Duration.ofSeconds(20)).until(() -> status(c5).equals("APPLIED"));
        assertThat(count("SELECT count(*) FROM data2flow_action.driver_circuits WHERE driver_id = 9 AND state = 'CLOSED'")).isEqualTo(1);
        await().atMost(Duration.ofSeconds(20)).until(() -> events("driver.circuit.closed").size() == 1);
    }
}
