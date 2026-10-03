package net.java21.data2flow.action.actuation.driver;

import net.java21.data2flow.contracts.capability.StandardCapabilities;
import net.java21.data2flow.contracts.capability.CapabilityDefinition;
import net.java21.data2flow.contracts.message.event.DeviceCommandAck;
import net.java21.data2flow.contracts.message.event.DeviceStateReported;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

/**
 * 드라이버 계약 테스트 키트(ACT-03.01, TC-ACT-066, BR-ACT-19, ACT-api §5.3). SPI가 action에 있으므로 키트도 action이 둔다.
 * 새 드라이버는 이 클래스를 상속한 {@code *DriverContractTest}가 통과해야 등록한다.
 *
 * <p>검사: 지원 기능 선언, execute → 시간 안에 ack(표준 이벤트로 정규화), 같은 commandId 재호출 시 효과 1회, 응답 없음은 예외 없이 끝나고
 * 창구가 TIMEOUT으로 처리할 수 있음, 오류는 예외 대신 FAILED(reason), 상태 보고에 버전, 연결 확인.
 */
public abstract class DriverContractTest {

    /** 장비(또는 장비를 흉내 내는 상대) */
    public interface DevicePeer {
        /** 명령에 응답할지(false = 응답 실패 확률 100%) */
        void respond(boolean respond);

        /** 장비에 실제로 적용된 횟수 */
        int effects(UUID commandId);

        /** 장비가 상태를 보고한다 */
        void emitState(Map<String, Map<String, Object>> capabilities);

        /** 연결 장애를 만든다 */
        void breakConnection();
    }

    /** 드라이버가 정규화해 넘긴 이벤트 */
    public static final class RecordingSink implements DriverEventSink {
        public final List<DeviceCommandAck> acks = new CopyOnWriteArrayList<>();
        public final List<DeviceStateReported> reports = new CopyOnWriteArrayList<>();

        @Override
        public void ack(long organizationId, DeviceCommandAck ack) {
            acks.add(ack);
        }

        @Override
        public void reported(long organizationId, DeviceStateReported state) {
            reports.add(state);
        }
    }

    protected abstract DeviceDriver driver();

    protected abstract DriverDevice device();

    protected abstract RecordingSink sink();

    protected abstract DevicePeer peer();

    @BeforeEach
    void resetPeer() {
        peer().respond(true);
        sink().acks.clear();
        sink().reports.clear();
    }

    protected DriverCommand command(UUID id, Map<String, Object> args) {
        return new DriverCommand(id, device(), "Switch", "set", args, Instant.parse("2030-01-01T00:00:00Z"), "k-" + id, 1);
    }

    @Test
    @DisplayName("[ACT-03.01][TC-ACT-066] 지원 기능을 선언하고, 모두 표준 기능 카탈로그에 있다")
    void declaresSupportedCapabilities() {
        Set<String> standard = StandardCapabilities.all().stream().map(CapabilityDefinition::name).collect(Collectors.toSet());
        assertThat(driver().supportedCapabilities()).isNotEmpty();
        assertThat(standard).containsAll(driver().supportedCapabilities());
        assertThat(driver().type()).isNotBlank();
    }

    @Test
    @DisplayName("[ACT-03.01][TC-ACT-066] execute → 응답 시간 안에 ack가 표준 이벤트(device.command.ack)로 온다")
    void executeAcksWithinTimeout() {
        UUID id = UUID.randomUUID();
        DriverResult r = driver().execute(command(id, Map.of("on", true)));

        assertThat(r.status()).isIn(DriverResult.Status.ACCEPTED, DriverResult.Status.ACKED);
        if (r.status() == DriverResult.Status.ACCEPTED) {
            await().atMost(driver().minResponseTimeout()).until(() -> sink().acks.stream().anyMatch(a -> a.commandId().equals(id.toString())));
            DeviceCommandAck ack = sink().acks.stream().filter(a -> a.commandId().equals(id.toString())).findFirst().orElseThrow();
            assertThat(ack.result()).isEqualTo(DeviceCommandAck.Result.ACKED);
            assertThat(ack.deviceId()).isEqualTo(device().deviceId());
        }
        assertThat(driver().minResponseTimeout()).isLessThanOrEqualTo(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("[ACT-03.01][TC-ACT-066] 같은 commandId로 다시 불러도 장비 효과는 1회")
    void sameCommandIdOnce() {
        UUID id = UUID.randomUUID();
        driver().execute(command(id, Map.of("on", true)));
        driver().execute(command(id, Map.of("on", true)));

        await().atMost(Duration.ofSeconds(5)).until(() -> peer().effects(id) >= 1);
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).until(() -> peer().effects(id) == 1);
    }

    @Test
    @DisplayName("[ACT-03.01][TC-ACT-066] 장비가 응답하지 않으면 예외 없이 끝나고 ack가 오지 않는다(창구가 TIMEOUT 처리)")
    void noResponseNoAck() {
        peer().respond(false);
        UUID id = UUID.randomUUID();

        DriverResult r = driver().execute(command(id, Map.of("on", false)));

        assertThat(r.status()).isNotEqualTo(DriverResult.Status.ACKED);
        await().during(Duration.ofMillis(500)).atMost(Duration.ofSeconds(2))
                .until(() -> sink().acks.stream().noneMatch(a -> a.commandId().equals(id.toString())));
    }

    @Test
    @DisplayName("[ACT-03.01][TC-ACT-066] 상태 보고는 표준 이벤트(device.state.reported)로 오고 버전을 가진다")
    void stateReportHasVersion() {
        driver().subscribeState(device(), (deviceId, state) -> {
        });
        peer().emitState(Map.of("Switch", Map.of("on", true)));

        await().atMost(Duration.ofSeconds(5)).until(() -> !sink().reports.isEmpty() || driver().getState(device()).isPresent());
        if (!sink().reports.isEmpty()) {
            assertThat(sink().reports.get(0).version()).isPositive();
            assertThat(sink().reports.get(0).capabilities()).containsKey("Switch");
        } else {
            assertThat(driver().getState(device()).orElseThrow().version()).isPositive();
        }
    }

    @Test
    @DisplayName("[ACT-03.01][TC-ACT-066] 연결 확인이 성공하고 지원 기능을 돌려준다")
    void healthCheck() {
        DriverHealth h = driver().healthCheck(device().config());

        assertThat(h.ok()).as(h.message()).isTrue();
        assertThat(h.capabilities()).isEqualTo(driver().supportedCapabilities());
    }

    /** 오류 매핑은 마지막에 연결을 끊으므로 하위 클래스가 순서를 정해 부른다 */
    protected void assertErrorsAreFailedResults() {
        peer().breakConnection();
        UUID id = UUID.randomUUID();
        assertThatCode(() -> driver().execute(command(id, Map.of("on", true)))).doesNotThrowAnyException();
        DriverResult r = driver().execute(command(UUID.randomUUID(), Map.of("on", true)));
        assertThat(r.status()).isEqualTo(DriverResult.Status.FAILED);
        assertThat(r.reason()).isNotBlank();
    }
}
