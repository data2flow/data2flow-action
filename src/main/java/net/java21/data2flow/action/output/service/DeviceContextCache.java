package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.output.domain.DeviceContext;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 기기 맥락 캐시(core API-DSC-74, 60초). 설정 변경 DEVICE·SPACE·GROUP을 받으면 모두 지운다. 없는 것은 한 번에 모아 읽고(최대 500개씩),
 * core가 모르는 기기는 ID만 있는 맥락으로 둔다(이름 대신 externalId, 공간·그룹 조건에는 걸리지 않음).
 */
public class DeviceContextCache {

    private static final int CHUNK = 500;

    private final OutputCoreClient core;
    private final Clock clock;
    private final Duration ttl;
    private final Map<Long, Entry> cache = new ConcurrentHashMap<>();

    private record Entry(DeviceContext context, Instant expiresAt) {
    }

    public DeviceContextCache(OutputCoreClient core, Clock clock, Duration ttl) {
        this.core = core;
        this.clock = clock;
        this.ttl = ttl;
    }

    /** 기기 ID → 맥락. core 호출이 실패하면 예외(소비자가 묶음을 다시 시도) */
    public Map<Long, DeviceContext> get(Map<Long, Long> deviceToOrganization) {
        Instant now = clock.instant();
        Map<Long, DeviceContext> out = new HashMap<>();
        List<Long> missing = new ArrayList<>();
        deviceToOrganization.keySet().forEach(id -> {
            Entry e = cache.get(id);
            if (e != null && now.isBefore(e.expiresAt())) {
                out.put(id, e.context());
            } else {
                missing.add(id);
            }
        });
        for (int i = 0; i < missing.size(); i += CHUNK) {
            Collection<Long> chunk = missing.subList(i, Math.min(missing.size(), i + CHUNK));
            Map<Long, DeviceContext> loaded = new HashMap<>();
            core.deviceContexts(chunk).forEach(c -> loaded.put(c.deviceId(), c));
            for (Long id : chunk) {
                DeviceContext c = loaded.getOrDefault(id, DeviceContext.unknown(id, deviceToOrganization.get(id)));
                cache.put(id, new Entry(c, now.plus(ttl)));
                out.put(id, c);
            }
        }
        return out;
    }

    public void invalidateAll() {
        cache.clear();
    }
}
