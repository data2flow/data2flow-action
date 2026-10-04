package net.java21.data2flow.action.sink.service;

import net.java21.data2flow.action.sink.connector.SinkConnection;
import net.java21.data2flow.action.sink.connector.SinkConnectorRegistry;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sink 연결 정의 캐시(최대 {@code ttl}, 기본 5분). {@code data2flow.config} SINK_CONNECTION(그 연결)·UNKNOWN·SETTING(전체)으로 지우고,
 * 지울 때 그 연결의 JDBC 풀도 닫는다(다음 쓰기에서 새 설정으로 다시 만든다).
 */
public class SinkConnectionCache {

    private final SinkConnectionClient client;
    private final SinkConnectorRegistry connectors;
    private final Clock clock;
    private final Duration ttl;
    private final Map<Long, Entry> entries = new ConcurrentHashMap<>();

    private record Entry(SinkConnection connection, Instant loadedAt) {
    }

    public SinkConnectionCache(SinkConnectionClient client, SinkConnectorRegistry connectors, Clock clock, Duration ttl) {
        this.client = client;
        this.connectors = connectors;
        this.clock = clock;
        this.ttl = ttl;
    }

    public Optional<SinkConnection> find(long connectionId) {
        Instant now = clock.instant();
        Entry e = entries.get(connectionId);
        if (e != null && now.isBefore(e.loadedAt().plus(ttl))) {
            return Optional.of(e.connection());
        }
        Optional<SinkConnection> loaded = client.find(connectionId);
        loaded.ifPresentOrElse(c -> entries.put(connectionId, new Entry(c, now)), () -> invalidate(connectionId));
        return loaded;
    }

    public void invalidate(long connectionId) {
        entries.remove(connectionId);
        connectors.release(connectionId);
    }

    public void invalidateAll() {
        entries.keySet().forEach(this::invalidate);
    }
}
