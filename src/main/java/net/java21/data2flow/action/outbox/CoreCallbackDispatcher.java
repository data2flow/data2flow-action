package net.java21.data2flow.action.outbox;

import net.java21.data2flow.action.outbox.OutboxRepository.OutboxMessage;
import net.java21.data2flow.contracts.identity.DataflowHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

/** core 내부 콜백(감사 API-IAM-39 등). 2xx면 성공, 그 밖은 예외(다음 주기에 다시 보냄). 토큰 없이 {@code X-CALLER-SERVICE}(ADR-021) */
public class CoreCallbackDispatcher implements OutboxDispatcher {

    private final RestClient core;

    public CoreCallbackDispatcher(RestClient core) {
        this.core = core;
    }

    @Override
    public boolean supports(OutboxMessage message) {
        return OutboxWriter.KIND_CALLBACK.equals(message.kind()) && OutboxWriter.CORE_CALLBACK.equals(message.exchange());
    }

    @Override
    public void dispatch(OutboxMessage message) {
        Integer status = core.post().uri(message.routingKey())
                .header(DataflowHeaders.CALLER_SERVICE, "data2flow-action")
                .contentType(MediaType.APPLICATION_JSON)
                .body(message.payload())
                .exchange((req, res) -> res.getStatusCode().value());
        if (status == null || status < 200 || status >= 300) {
            throw new IllegalStateException("core 콜백 실패: HTTP " + status + " " + message.routingKey());
        }
    }
}
