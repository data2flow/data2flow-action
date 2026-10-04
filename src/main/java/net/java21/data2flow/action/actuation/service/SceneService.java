package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.Command;
import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.actuation.domain.InterlockEvaluator;
import net.java21.data2flow.action.actuation.domain.SceneDefinition;
import net.java21.data2flow.action.actuation.domain.SceneRules;
import net.java21.data2flow.action.actuation.repository.CommandRepository;
import net.java21.data2flow.action.actuation.repository.SceneBulkRepository;
import net.java21.data2flow.action.actuation.repository.ShadowRepository;
import net.java21.data2flow.action.common.ActionErrorCode;
import net.java21.data2flow.action.common.CoreClient;
import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.command.ActionIdempotencyKeys;
import net.java21.data2flow.contracts.command.CommandPriority;
import net.java21.data2flow.contracts.command.CommandSource;
import net.java21.data2flow.contracts.command.CommandStatus;
import net.java21.data2flow.contracts.error.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 장면(ACT-05.01·05.03, BR-ACT-16, BR-ACT-04). 정의는 core(API-ACT-47), 실행 기록은 action({@code scene_runs}).
 *
 * <ul>
 *   <li>실행: 항목(≤ 100)을 기기별로 펼쳐 제어 창구로 명령을 낸다. 출처는 SCENE(장면 실행 ID + 실행 출처), 우선순위는 실행 출처를 따른다
 *       (화면 = MANUAL, 예약 = SCHEDULE, 플로우 = AUTO, AI = AI, BR-ACT-24). 일부가 막혀도 나머지는 적용한다(PARTIAL).</li>
 *   <li>미리보기: 기기별 현재 → 목표, 이미 같으면 "변경 없음"(BR-ACT-04), 인터락에 걸릴 항목과 오프라인 기기를 표시한다.</li>
 *   <li>실행 상태는 명령 상태로 계산해 끝나면 저장한다(SUCCEEDED·PARTIAL·FAILED).</li>
 * </ul>
 */
public class SceneService {

    private static final Logger log = LoggerFactory.getLogger(SceneService.class);

    private final CoreClient core;
    private final ControlProfileService profiles;
    private final ShadowRepository shadows;
    private final InterlockService interlocks;
    private final SceneBulkRepository repository;
    private final CommandRepository commands;
    private final ControlFacade facade;
    private final Clock clock;

    public SceneService(CoreClient core, ControlProfileService profiles, ShadowRepository shadows, InterlockService interlocks,
                        SceneBulkRepository repository, CommandRepository commands, ControlFacade facade, Clock clock) {
        this.core = core;
        this.profiles = profiles;
        this.shadows = shadows;
        this.interlocks = interlocks;
        this.repository = repository;
        this.commands = commands;
        this.facade = facade;
        this.clock = clock;
    }

    /** 펼친 항목 하나: 기기 + 기능 + 목표 */
    record Target(long deviceId, String capability, Map<String, Object> desired) {
    }

    /** 장면 정의를 읽는다(다른 조직·없음이면 SCENE_NOT_FOUND, 항목 초과면 SCENE_ITEM_LIMIT_EXCEEDED) */
    public SceneDefinition load(long organizationId, long sceneId) {
        SceneDefinition scene = core.scene(sceneId).filter(s -> s.organizationId() == organizationId)
                .orElseThrow(() -> new BusinessException(ActionErrorCode.SCENE_NOT_FOUND));
        if (scene.items().size() > SceneRules.MAX_ITEMS) {
            throw new BusinessException(ActionErrorCode.SCENE_ITEM_LIMIT_EXCEEDED);
        }
        return scene;
    }

    /**
     * 장면 실행. 반환: 장면 실행 ID.
     *
     * @param runOrigin 실행 출처(USER·SCHEDULE·FLOW·AI). 우선순위를 정한다
     * @param runKey    실행 멱등 키(같은 키로 다시 실행하면 기기마다 한 번만)
     */
    public long run(long organizationId, long sceneId, CommandSource runOrigin, String runKey) {
        SceneDefinition scene = load(organizationId, sceneId);
        CommandPriority.forScene(runOrigin.type());   // SCENE·UNKNOWN 출처 거부
        long runId = repository.insertSceneRun(organizationId, sceneId, Json.toMap(runOrigin), clock.instant());
        CommandSource source = SceneRules.sceneSource(runOrigin, Long.toString(runId));
        List<Map<String, Object>> results = new ArrayList<>();
        for (Target t : expand(scene)) {
            String key = ActionIdempotencyKeys.of("scene", runKey, Long.toString(t.deviceId()), t.capability());
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("deviceId", Long.toString(t.deviceId()));
            try {
                Outcome o = facade.submit(new CommandRequest(organizationId, t.deviceId(), t.capability(), "set", t.desired(), source, null, key,
                        null, null, scene.spaceId(), false, false));
                r.put("commandId", o.command().id().toString());
                r.put("status", o.command().status().name());
                if (o.command().statusReason() != null) {
                    r.put("reason", o.command().statusReason());
                }
            } catch (BusinessException e) {
                r.put("status", "FAILED");
                r.put("reason", e.getErrorCode().code());
                log.warn("장면 항목 실패 scene={} device={}: {}", sceneId, t.deviceId(), e.getErrorCode().code());
            }
            results.add(r);
        }
        repository.saveSceneRun(organizationId, runId, SceneRules.RunStatus.RUNNING.name(), results, null);
        refresh(organizationId, runId);
        return runId;
    }

    /** 실행 상태(API-ACT-11 {@code GET /scene-runs/{id}}): 명령 상태로 다시 계산하고 끝났으면 저장한다 */
    public Map<String, Object> refresh(long organizationId, long runId) {
        SceneBulkRepository.SceneRun run = repository.findSceneRun(organizationId, runId)
                .orElseThrow(() -> new BusinessException(ActionErrorCode.SCENE_NOT_FOUND));
        List<Command> list = commands.findBySource(organizationId, "SCENE", Long.toString(runId));
        Map<String, Command> byId = new LinkedHashMap<>();
        list.forEach(c -> byId.put(c.id().toString(), c));
        List<Map<String, Object>> results = new ArrayList<>();
        List<CommandStatus> statuses = new ArrayList<>();
        for (Object o : run.results()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> r = new LinkedHashMap<>((Map<String, Object>) o);
            Command c = r.get("commandId") == null ? null : byId.get(r.get("commandId").toString());
            if (c != null) {
                r.put("status", c.status().name());
                if (c.statusReason() != null) {
                    r.put("reason", c.statusReason());
                }
                statuses.add(c.status());
            } else {
                statuses.add(CommandStatus.FAILED);
            }
            results.add(r);
        }
        SceneRules.RunStatus status = SceneRules.status(statuses);
        if (status != SceneRules.RunStatus.RUNNING && !status.name().equals(run.status())) {
            repository.saveSceneRun(organizationId, runId, status.name(), results, clock.instant());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("sceneRunId", Long.toString(runId));
        out.put("sceneId", Long.toString(run.sceneId()));
        out.put("status", status.name());
        out.put("results", results);
        return out;
    }

    /** 미리보기(API-ACT-12) */
    public Map<String, Object> preview(long organizationId, long sceneId) {
        SceneDefinition scene = load(organizationId, sceneId);
        List<Map<String, Object>> items = new ArrayList<>();
        for (Target t : expand(scene)) {
            Optional<ControlProfile> p = profiles.find(t.deviceId());
            var row = shadows.findByDeviceIdAndOrganizationId(t.deviceId(), organizationId);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("deviceId", Long.toString(t.deviceId()));
            m.put("name", p.map(ControlProfile::name).orElse(null));
            m.put("capability", t.capability());
            m.put("current", row.map(r -> r.shadow().reported().getOrDefault(t.capability(), Map.of())).orElse(Map.of()));
            m.put("target", t.desired());
            m.put("willChange", row.map(r -> !r.shadow().isApplied(t.capability(), t.desired())).orElse(true));
            if (p.isPresent()) {
                InterlockEvaluator.Decision d = interlocks.evaluate(p.get(), CommandPriority.MANUAL, t.capability(), "set", t.desired());
                if (d.blocked()) {
                    m.put("predictedBlock", Map.of("reason", "INTERLOCK", "message", d.message()));
                }
            }
            m.put("offline", row.map(ShadowRepository.ShadowRow::offline).orElse(false));
            items.add(m);
        }
        return Map.of("items", items);
    }

    /** 항목을 기기별로 펼친다(관계 대상은 core API-DEV-128) */
    List<Target> expand(SceneDefinition scene) {
        List<Target> out = new ArrayList<>();
        for (SceneDefinition.Item item : scene.items()) {
            SceneDefinition.Target t = item.target();
            if (t == null) {
                continue;
            }
            if (t.deviceId() != null) {
                out.add(new Target(t.deviceId(), item.capability(), item.desired()));
            } else if (t.spaceId() != null) {
                String capability = t.capability() == null ? item.capability() : t.capability();
                for (Long d : core.spaceDevices(t.spaceId(), t.relation() == null ? "controls" : t.relation(), capability,
                        Boolean.TRUE.equals(t.includeChildren()))) {
                    out.add(new Target(d, item.capability(), item.desired()));
                }
            }
        }
        return out;
    }
}
