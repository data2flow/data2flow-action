package net.java21.data2flow.action.output.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 출력 연결 1분 발송 지표(sent·failed·retried·lagMs, core {@code output_delivery_stats}). 메모리에 모았다가 1분마다(종료할 때도) core
 * API-DSC-75로 보낸다. 보내지 못하면 다음에 합쳐 다시 보내고, 1시간 넘게 못 보낸 분은 버린다(지표라 유실 허용).
 */
public class OutputStats {

    private static final Logger log = LoggerFactory.getLogger(OutputStats.class);

    private final OutputCoreClient core;
    private final Clock clock;
    private final Map<Key, Counters> pending = new ConcurrentHashMap<>();

    record Key(long outputId, long organizationId, Instant minute) {
    }

    static final class Counters {
        long sent;
        long failed;
        long retried;
        Long lagMs;

        synchronized void add(long s, long f, long r, Long lag) {
            sent += s;
            failed += f;
            retried += r;
            if (lag != null) {
                lagMs = lagMs == null ? lag : Math.max(lagMs, lag);
            }
        }
    }

    public OutputStats(OutputCoreClient core, Clock clock) {
        this.core = core;
        this.clock = clock;
    }

    public void sent(long outputId, long organizationId, int count, int retried, Long lagMs) {
        counters(outputId, organizationId).add(count, 0, retried, lagMs);
    }

    public void failed(long outputId, long organizationId, int count) {
        counters(outputId, organizationId).add(0, count, 0, null);
    }

    private Counters counters(long outputId, long organizationId) {
        Instant minute = clock.instant().truncatedTo(ChronoUnit.MINUTES);
        return pending.computeIfAbsent(new Key(outputId, organizationId, minute), k -> new Counters());
    }

    /** 끝난 분(또는 {@code all}이면 지금 분까지)을 보낸다. 보낸 항목 수 */
    public int flush(boolean all) {
        Instant now = clock.instant();
        Instant current = now.truncatedTo(ChronoUnit.MINUTES);
        Map<Key, Counters> taken = new LinkedHashMap<>();
        for (Key k : List.copyOf(pending.keySet())) {
            if (all || k.minute().isBefore(current)) {
                Counters c = pending.remove(k);
                if (c != null) {
                    taken.put(k, c);
                }
            }
        }
        if (taken.isEmpty()) {
            return 0;
        }
        List<Map<String, Object>> items = new ArrayList<>();
        taken.forEach((k, c) -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("outputId", Long.toString(k.outputId()));
            item.put("organizationId", Long.toString(k.organizationId()));
            item.put("minute", k.minute().toString());
            item.put("sent", c.sent);
            item.put("failed", c.failed);
            item.put("retried", c.retried);
            item.put("lagMs", c.lagMs);
            items.add(item);
        });
        try {
            core.postStats(items);
            return items.size();
        } catch (RuntimeException e) {
            log.warn("출력 발송 지표를 보내지 못했습니다(다음에 다시): {}", e.getMessage());
            Instant oldest = now.minus(Duration.ofHours(1));
            taken.forEach((k, c) -> {
                if (!k.minute().isBefore(oldest)) {
                    pending.computeIfAbsent(k, x -> new Counters()).add(c.sent, c.failed, c.retried, c.lagMs);
                }
            });
            return 0;
        }
    }
}
