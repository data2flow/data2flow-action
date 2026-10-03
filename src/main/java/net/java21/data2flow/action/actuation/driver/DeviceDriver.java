package net.java21.data2flow.action.actuation.driver;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/**
 * 드라이버 SPI(ACT-03.01, ACT-api §5.3, ADR-009). 표준 명령을 벤더·프로토콜 호출로 바꾸고, 응답·상태 보고를 표준 이벤트
 * {@code device.command.ack}·{@code device.state.reported}로 정규화해 {@link DriverEventSink}에 넘긴다(BR-ACT-25).
 *
 * <p>드라이버는 제어 창구({@code ControlFacade})만 부른다(ACT-02.01: 드라이버를 직접 부르는 경로는 없다). 새 드라이버는 테스트 키트
 * {@code DriverContractTest}를 통과해야 등록한다(BR-ACT-19).
 *
 * <p>계약:
 * <ul>
 *   <li>{@link #execute}는 예외를 던지지 않고 결과로 돌려준다({@code FAILED(reason)}).</li>
 *   <li>같은 {@code commandId}로 다시 불러도 장비에 효과는 한 번만 난다(재시도·재전달 안전).</li>
 *   <li>{@link #minResponseTimeout()} 안에 ack가 오지 않으면 창구가 TIMEOUT으로 끝낸다.</li>
 *   <li>상태 보고에는 버전이 있다(BR-ACT-05).</li>
 * </ul>
 */
public interface DeviceDriver {

    /** 종류(예: {@code VIRTUAL}, {@code MQTT}). core {@code drivers.type}과 같다 */
    String type();

    /** 이 드라이버가 지원하는 표준 기능 */
    Set<String> supportedCapabilities();

    /** 연결·인증 확인(API-ACT-31) */
    DriverHealth healthCheck(DriverConfig config);

    /** 표준 명령 → 벤더 호출. 반환: ACCEPTED(비동기 응답 예정) | ACKED | FAILED(reason) */
    DriverResult execute(DriverCommand command);

    /** 폴링용 현재 상태. 지원하지 않으면 빈 값 */
    Optional<ReportedState> getState(DriverDevice device);

    /** push 지원 시 상태 구독(MQTT state 토픽, 벤더 이벤트). push가 없으면 아무것도 하지 않는다 */
    void subscribeState(DriverDevice device, StateListener listener);

    /** 응답을 기다리는 최소 시간(ack 기한의 하한) */
    default Duration minResponseTimeout() {
        return Duration.ofSeconds(30);
    }
}
