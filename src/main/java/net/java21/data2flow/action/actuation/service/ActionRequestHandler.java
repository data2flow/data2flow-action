package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.ExecutionRepository;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.contracts.command.ActionKind;
import net.java21.data2flow.contracts.command.CommandPayload;
import net.java21.data2flow.contracts.command.CommandTarget;
import net.java21.data2flow.contracts.error.BusinessException;
import net.java21.data2flow.contracts.error.CommonErrorCode;
import net.java21.data2flow.contracts.message.ActionRequest;
import net.java21.data2flow.contracts.message.MessageFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 행동 요청 처리(ACT-api §5.1, EVT-FLW-05): {@code action.commands} 큐의 {@code ActionRequest} kind=COMMAND를 제어 창구로 넘긴다.
 *
 * <ul>
 *   <li>멱등(BR-ACT-02): {@code executed_actions}에 키가 있으면 실행하지 않고 처음 결과로 EVT-ACT-01을 다시 낸다.</li>
 *   <li>관계 대상({@code {spaceId, relation:"controls", capability}})은 실행 시점에 기기별 명령으로 펼친다(TC-ACT-039).
 *       기기별 키는 요청 키 + 기기 ID라 다시 처리해도 기기마다 한 번만 실행된다.</li>
 *   <li>{@code validUntil}이 지난 요청은 실행하지 않고 FAILED(EXPIRED)로 남긴다.</li>
 *   <li>형식 오류·모르는 종류·출처 누락(승인 없는 AI 등, BR-ACT-15)은 {@link MessageFormatException}: 재시도 없이 DLQ.</li>
 * </ul>
 * 처리(DB 커밋)가 끝난 뒤에 호출 쪽이 큐에 ACK한다.
 */
public class ActionRequestHandler {

    private static final Logger log = LoggerFactory.getLogger(ActionRequestHandler.class);
    public static final String CONSUMER = "action.commands";

    private final ControlFacade facade;
    private final CommandRepository commands;
    private final ExecutionRepository executions;
    private final CommandEvents events;
    private final ControlProfileService profiles;
    private final CoreClient core;
    private final TransactionTemplate tx;
    private final Clock clock;

    public ActionRequestHandler(ControlFacade facade, CommandRepository commands, ExecutionRepository executions, CommandEvents events,
                                ControlProfileService profiles, CoreClient core, PlatformTransactionManager txManager, Clock clock) {
        this.facade = facade;
        this.commands = commands;
        this.executions = executions;
        this.events = events;
        this.profiles = profiles;
        this.core = core;
        this.tx = new TransactionTemplate(txManager);
        this.clock = clock;
    }

    /** 처리한 명령들(재요청이면 처음 명령들) */
    public List<Command> handle(ActionRequest req) {
        if (req.kind() != ActionKind.COMMAND) {
            // SCENE(장면 실행)은 ACT-05(M4). 지금은 처리할 수 없으므로 DLQ로 보내 사람이 확인한다
            throw new MessageFormatException("이 버전의 action이 처리하지 않는 행동 종류입니다: " + req.kind());
        }
        CommandPayload payload = req.commandPayload();
        String requestKey = IdempotencyKeys.action(req.source().type(), req.idempotencyKey());
        tx.executeWithoutResult(s -> executions.markProcessed(CONSUMER, req.messageId().toString(), clock.instant()));
        if (executions.findExecuted(req.organizationId(), requestKey).isPresent()) {
            return replay(req, requestKey, payload);
        }
        List<Long> devices = targets(payload.target());
        Instant now = clock.instant();
        boolean expired = req.expiredAt(now);
        List<Command> results = new ArrayList<>();
        for (long deviceId : devices) {
            String key = keyFor(requestKey, payload.target(), deviceId);
            CommandRequest cr = new CommandRequest(req.organizationId(), deviceId, payload.capability(), payload.command(), payload.args(),
                    req.source(), req.priority(), key, req.validUntil(), null, payload.target().spaceId(), false, expired);
            try {
                results.add(facade.submit(cr).command());
            } catch (BusinessException e) {
                if (e.getErrorCode() == CommonErrorCode.INVALID_REQUEST) {
                    throw new MessageFormatException("행동 요청 출처가 올바르지 않습니다: " + e.getErrors());
                }
                // 없는 기기·제어할 수 없는 기기: 플로우가 다시 보내도 결과가 같으므로 기록만 남기고 넘어간다
                log.warn("행동 요청 대상 기기를 제어할 수 없습니다 key={} device={} code={}", requestKey, deviceId, e.getErrorCode().code());
            }
        }
        String ref = results.size() == 1 ? results.get(0).id().toString() : "n=" + results.size();
        String status = results.size() == 1 ? results.get(0).status().name() : (results.isEmpty() ? "NO_TARGET" : "MULTIPLE");
        tx.executeWithoutResult(s -> executions.recordExecuted(req.organizationId(), requestKey, ActionKind.COMMAND.name(), ref, status,
                clock.instant()));
        return results;
    }

    private List<Command> replay(ActionRequest req, String requestKey, CommandPayload payload) {
        List<String> keys = new ArrayList<>();
        if (payload.target().isDevice()) {
            keys.add(requestKey);
        } else {
            for (long deviceId : targets(payload.target())) {
                keys.add(IdempotencyKeys.expanded(requestKey, deviceId));
            }
        }
        List<Command> found = commands.findByKeys(req.organizationId(), keys);
        tx.executeWithoutResult(s -> found.forEach(c -> events.republish(c, profiles.spaceOf(c.deviceId()), req.messageId().toString())));
        return found;
    }

    private List<Long> targets(CommandTarget target) {
        if (target.isDevice()) {
            return List.of(target.deviceId());
        }
        return core.spaceDevices(target.spaceId(), target.relation(), target.capability(), Boolean.TRUE.equals(target.includeChildren()));
    }

    private static String keyFor(String requestKey, CommandTarget target, long deviceId) {
        return target.isDevice() ? requestKey : IdempotencyKeys.expanded(requestKey, deviceId);
    }
}
