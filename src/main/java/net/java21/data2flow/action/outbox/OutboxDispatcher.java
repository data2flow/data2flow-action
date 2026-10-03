package net.java21.data2flow.action.outbox;

import net.java21.data2flow.action.outbox.OutboxRepository.OutboxMessage;

/** 아웃박스 한 행을 보낸다. 대상이 확인해야 정상 반환하고, 아니면 예외(릴레이가 다음 주기에 다시 보낸다) */
public interface OutboxDispatcher {

    boolean supports(OutboxMessage message);

    void dispatch(OutboxMessage message) throws Exception;
}
