package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.output.domain.HostGuard;
import net.java21.data2flow.action.output.domain.OutputConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 출력 연결 정의 사본(core API-DSC-73). 30초마다(또는 설정 변경 OUTPUT을 받으면 바로) 다시 읽고, 바뀌지 않았으면(204) 그대로 둔다.
 * core가 잠시 응답하지 않으면 마지막으로 받은 정의로 계속 돈다. 공용 브로커 주소 연결은 쓰지 않는다(CLAUDE.md §5).
 */
public class OutputConnectionRegistry {

    private static final Logger log = LoggerFactory.getLogger(OutputConnectionRegistry.class);

    private final OutputCoreClient core;
    private final Clock clock;
    private final Duration refresh;
    private final List<String> deniedHosts;
    private final Runnable onChange;
    private volatile Map<Long, OutputConnection> byId = Map.of();
    private volatile Long version;
    private volatile Instant loadedAt;
    private volatile boolean dirty = true;

    public OutputConnectionRegistry(OutputCoreClient core, Clock clock, Duration refresh, List<String> deniedHosts, Runnable onChange) {
        this.core = core;
        this.clock = clock;
        this.refresh = refresh;
        this.deniedHosts = List.copyOf(deniedHosts);
        this.onChange = onChange == null ? () -> {
        } : onChange;
    }

    /** 오래됐거나 변경 통지를 받았으면 다시 읽는다. 실패하면 이전 정의 유지 */
    public void refreshIfStale() {
        Instant now = clock.instant();
        if (!dirty && loadedAt != null && now.isBefore(loadedAt.plus(refresh))) {
            return;
        }
        refresh(now);
    }

    synchronized void refresh(Instant now) {
        try {
            Optional<OutputCoreClient.Runtime> r = core.runtime(dirty ? null : version);
            r.ifPresent(rt -> {
                byId = rt.connections().stream().collect(Collectors.toUnmodifiableMap(OutputConnection::id, Function.identity(), (a, b) -> b));
                version = rt.version();
                rt.connections().stream().filter(c -> HostGuard.denied(c.url(), deniedHosts))
                        .forEach(c -> log.warn("출력 연결 {} 주소는 접속 금지(공용 브로커, CLAUDE.md §5)라 쓰지 않습니다", c.id()));
                onChange.run();
            });
            loadedAt = now;
            dirty = false;
        } catch (RuntimeException e) {
            log.warn("출력 연결 정의를 읽지 못했습니다(이전 정의로 계속): {}", e.getMessage());
        }
    }

    /** 설정 변경 OUTPUT·SETTING 수신 */
    public void invalidate() {
        dirty = true;
    }

    /** 조직의 켜진·접속 허용 연결 */
    public List<OutputConnection> active(long organizationId) {
        return byId.values().stream()
                .filter(c -> c.organizationId() == organizationId && c.enabled() && !HostGuard.denied(c.url(), deniedHosts))
                .toList();
    }

    public boolean hasActive(long organizationId) {
        return byId.values().stream().anyMatch(c -> c.organizationId() == organizationId && c.enabled());
    }

    /** ID로(꺼진 것 포함). 정의에 없으면(삭제) 빈 값 */
    public Optional<OutputConnection> find(long id) {
        return Optional.ofNullable(byId.get(id));
    }

    /** 정의를 한 번이라도 받았는가(못 받았으면 대기 행을 지우지 않는다) */
    public boolean loaded() {
        return version != null;
    }
}
