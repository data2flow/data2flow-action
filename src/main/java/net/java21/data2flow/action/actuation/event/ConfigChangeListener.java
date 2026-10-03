package net.java21.data2flow.action.actuation.event;

import net.java21.data2flow.action.actuation.service.ControlProfileService;
import net.java21.data2flow.action.actuation.service.SandboxRegistry;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

/**
 * 설정 변경 수신(fanout {@code data2flow.config}, 인스턴스별 임시 큐, architecture.md §4.5). 내용이 아니라 종류·ID만 보고 캐시를 지운다.
 *
 * <table>
 *   <tr><th>entityType</th><th>동작</th></tr>
 *   <tr><td>DEVICE·ATTRIBUTE</td><td>그 기기의 제어 프로필</td></tr>
 *   <tr><td>MODEL·SPACE·SETTING</td><td>모든 제어 프로필(모델 기능·제약, 조직 제어 설정)</td></tr>
 *   <tr><td>SIM_SANDBOX</td><td>샌드박스 목록을 다시 읽음(1초 안 반영, TC-ACT-030)</td></tr>
 *   <tr><td>UNKNOWN</td><td>모든 제어 프로필(이 코드가 모르는 CAPABILITY·DRIVER 같은 새 종류일 수 있으므로 안전하게)</td></tr>
 * </table>
 * 재연결하면 놓친 메시지가 있을 수 있으므로 모두 지운다.
 */
public class ConfigChangeListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangeListener.class);

    private final ControlProfileService profiles;
    private final SandboxRegistry sandbox;
    private final MessageCodec codec = MessageCodec.create();

    public ConfigChangeListener(ControlProfileService profiles, SandboxRegistry sandbox) {
        this.profiles = profiles;
        this.sandbox = sandbox;
    }

    @Override
    public void onMessage(Message message) {
        ConfigChangedMessage change;
        try {
            change = codec.read(message.getBody(), ConfigChangedMessage.class);
        } catch (RuntimeException e) {
            log.warn("읽을 수 없는 설정 변경 메시지를 무시합니다: {}", e.getMessage());
            return;
        }
        apply(change);
    }

    public void apply(ConfigChangedMessage change) {
        switch (change.entityType()) {
            case DEVICE, ATTRIBUTE -> {
                try {
                    profiles.invalidate(Long.parseLong(change.id()));
                } catch (NumberFormatException e) {
                    profiles.invalidateAll();
                }
            }
            case MODEL, SPACE, SETTING, UNKNOWN -> profiles.invalidateAll();
            case SIM_SANDBOX -> {
                try {
                    sandbox.reload();
                } catch (RuntimeException e) {
                    log.warn("샌드박스 목록을 다시 읽지 못했습니다(다음 사용 때 다시 읽음): {}", e.toString());
                    sandbox.invalidate();
                }
            }
            default -> {
                // 다른 서비스용(SOURCE·SCRIPT·FLOW 등)
            }
        }
    }

    /** 재연결: 놓친 변경이 있을 수 있다 */
    public void invalidateAll() {
        profiles.invalidateAll();
        sandbox.invalidate();
    }
}
