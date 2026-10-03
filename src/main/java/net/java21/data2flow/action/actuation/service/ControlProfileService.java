package net.java21.data2flow.action.actuation.service;

import net.java21.data2flow.action.actuation.domain.ControlProfile;
import net.java21.data2flow.action.common.ActionProperties;
import net.java21.data2flow.action.common.CoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 제어 프로필 캐시(ACT domain-model 머리말: 정의는 core, action은 캐시). {@code data2flow.config}의 DEVICE·MODEL·SETTING 등을 받으면
 * 지우고(1초 안 반영), 메시지를 놓쳐도 {@code profileTtl}(30초) 뒤에는 다시 읽는다.
 */
public class ControlProfileService {

    private static final Logger log = LoggerFactory.getLogger(ControlProfileService.class);

    private final CoreClient core;
    private final Clock clock;
    private final ActionProperties properties;
    private final Map<Long, Entry> cache = new ConcurrentHashMap<>();

    private record Entry(Optional<ControlProfile> profile, Instant loadedAt) {
    }

    public ControlProfileService(CoreClient core, Clock clock, ActionProperties properties) {
        this.core = core;
        this.clock = clock;
        this.properties = properties;
    }

    /** 기기의 제어 프로필(없는 기기면 빈 값). core 장애면 예외(호출 쪽이 재시도) */
    public Optional<ControlProfile> find(long deviceId) {
        Instant now = clock.instant();
        Entry e = cache.get(deviceId);
        if (e != null && now.isBefore(e.loadedAt().plus(properties.profileTtl()))) {
            return e.profile();
        }
        Optional<ControlProfile> p = core.controlProfile(deviceId);
        cache.put(deviceId, new Entry(p, now));
        return p;
    }

    /** 이벤트용 공간 ID. 모르면 null(이벤트 발행을 막지 않는다) */
    public Long spaceOf(long deviceId) {
        try {
            return find(deviceId).map(ControlProfile::spaceId).orElse(null);
        } catch (RuntimeException ex) {
            log.debug("공간 ID를 읽지 못했습니다 device={}: {}", deviceId, ex.toString());
            return null;
        }
    }

    public void invalidate(long deviceId) {
        cache.remove(deviceId);
    }

    public void invalidateAll() {
        cache.clear();
    }
}
