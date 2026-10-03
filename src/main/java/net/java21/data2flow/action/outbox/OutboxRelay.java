package net.java21.data2flow.action.outbox;

import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.outbox.OutboxRepository.OutboxMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.util.List;

/**
 * 아웃박스 릴레이(ERD README §11.1). 보내지 않은 행을 {@code FOR UPDATE SKIP LOCKED}로 잠가 파드 여러 대가 나눠 보내고, 대상의 확인을
 * 받은 뒤 sent_at을 쓴다. 실패한 행은 다음 주기에 다시 보낸다(최소 1회). 자기 배포 행만 본다(ADR-030).
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final List<OutboxDispatcher> dispatchers;
    private final TransactionTemplate tx;
    private final ActionProperties properties;
    private final Clock clock;

    public OutboxRelay(OutboxRepository repository, List<OutboxDispatcher> dispatchers, PlatformTransactionManager txManager,
                       ActionProperties properties, Clock clock) {
        this.repository = repository;
        this.dispatchers = dispatchers;
        this.tx = new TransactionTemplate(txManager);
        this.properties = properties;
        this.clock = clock;
    }

    /** 한 묶음을 보낸다. 보낸 행 수 */
    public int relayOnce() {
        Integer sent = tx.execute(status -> {
            int count = 0;
            for (OutboxMessage message : repository.lockUnsent(properties.outbox().batchSize(), properties.env())) {
                try {
                    dispatcherFor(message).dispatch(message);
                    repository.markSent(message.id(), clock.instant());
                    count++;
                } catch (Exception ex) {
                    if (ex instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    log.warn("아웃박스 발송 실패 id={} kind={} attempts={}: {}", message.id(), message.kind(), message.attempts() + 1, ex.toString());
                    repository.markFailed(message.id(), ex.getClass().getSimpleName() + ": " + ex.getMessage());
                }
            }
            return count;
        });
        return sent == null ? 0 : sent;
    }

    /** 보낸 지 7일 지난 행 정리 */
    public int purgeSent() {
        return repository.deleteSentBefore(clock.instant().minus(properties.outbox().retention()));
    }

    private OutboxDispatcher dispatcherFor(OutboxMessage message) {
        return dispatchers.stream().filter(d -> d.supports(message)).findFirst()
                .orElseThrow(() -> new IllegalStateException("보낼 곳을 모르는 아웃박스: " + message.kind() + " " + message.exchange()));
    }
}
