package net.java21.data2flow.action.output.service;

import net.java21.data2flow.action.output.OutputFixtures;
import net.java21.data2flow.action.output.domain.DeviceContext;
import net.java21.data2flow.action.output.event.OutputConfigListener;
import net.java21.data2flow.action.support.MutableClock;
import net.java21.data2flow.contracts.message.ConfigChangedMessage;
import net.java21.data2flow.contracts.message.MessageCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 출력 연결 정의 사본·기기 맥락 캐시·지표·설정 변경 수신(DSC-04.01) */
class OutputServicesTest {

    private final MutableClock clock = new MutableClock(MutableClock.T0);
    private final OutputCoreClient core = mock(OutputCoreClient.class);

    @Test
    @DisplayName("[DSC-04.01] 연결 정의: 30초마다·변경 통지 때만 다시 읽고(204면 그대로), core가 죽으면 이전 정의 유지, 공용 브로커 연결은 쓰지 않음")
    void registry() {
        AtomicInteger changes = new AtomicInteger();
        OutputConnectionRegistry r = new OutputConnectionRegistry(core, clock, Duration.ofSeconds(30), List.of(), changes::incrementAndGet);
        when(core.runtime(null)).thenReturn(Optional.of(new OutputCoreClient.Runtime(5, List.of(
                OutputFixtures.parse(OutputFixtures.webhook(1, "https://h.example/in", Map.of(), Map.of(), Map.of())),
                OutputFixtures.parse(OutputFixtures.mqtt(2, "wss://iot-data.java21.net/mqtt", "d2f/{metric}", Map.of()))))));
        r.refreshIfStale();
        assertThat(r.loaded()).isTrue();
        assertThat(r.active(1)).extracting(c -> c.id()).containsExactly(1L);
        assertThat(r.find(2)).isPresent();
        assertThat(changes.get()).isEqualTo(1);
        r.refreshIfStale();
        verify(core, times(1)).runtime(null);
        clock.advanceBy(Duration.ofSeconds(31));
        when(core.runtime(5L)).thenReturn(Optional.empty());
        r.refreshIfStale();
        verify(core).runtime(5L);
        assertThat(changes.get()).isEqualTo(1);
        r.invalidate();
        when(core.runtime(null)).thenThrow(new IllegalStateException("down"));
        r.refreshIfStale();
        assertThat(r.active(1)).hasSize(1);
        assertThat(r.hasActive(9)).isFalse();
    }

    @Test
    @DisplayName("[DSC-04.01] 기기 맥락: 60초 캐시, 모르는 기기는 ID만 있는 맥락")
    void contexts() {
        DeviceContextCache cache = new DeviceContextCache(core, clock, Duration.ofSeconds(60));
        when(core.deviceContexts(anyList())).thenReturn(List.of(OutputFixtures.context()));
        Map<Long, DeviceContext> got = cache.get(Map.of(OutputFixtures.DEVICE, 1L, 999L, 1L));
        assertThat(got.get(OutputFixtures.DEVICE).spaceCode()).isEqualTo("R101");
        assertThat(got.get(999L).deviceName()).isNull();
        cache.get(Map.of(OutputFixtures.DEVICE, 1L));
        verify(core, times(1)).deviceContexts(anyList());
        cache.invalidateAll();
        cache.get(Map.of(OutputFixtures.DEVICE, 1L));
        verify(core, times(2)).deviceContexts(anyList());
    }

    @Test
    @DisplayName("[DSC-04.01] 1분 지표: 끝난 분만 보내고, 보내지 못하면 합쳐서 다음에 다시")
    void stats() {
        OutputStats stats = new OutputStats(core, clock);
        stats.sent(1, 1, 3, 1, 1500L);
        stats.failed(1, 1, 2);
        assertThat(stats.flush(false)).as("지금 분은 아직").isZero();
        clock.advanceBy(Duration.ofMinutes(1));
        doThrow(new IllegalStateException("down")).when(core).postStats(anyList());
        assertThat(stats.flush(false)).isZero();
        stats.sent(1, 1, 1, 0, 10L);
        org.mockito.Mockito.reset(core);
        assertThat(stats.flush(true)).isEqualTo(2);
        verify(core).postStats(any());
    }

    @Test
    @DisplayName("[DSC-04.01] 설정 변경: OUTPUT은 정의, DEVICE·SPACE·GROUP은 맥락, SETTING은 둘 다 다시 읽기. 깨진 메시지는 무시")
    void configListener() {
        OutputConnectionRegistry registry = mock(OutputConnectionRegistry.class);
        DeviceContextCache contexts = mock(DeviceContextCache.class);
        OutputConfigListener l = new OutputConfigListener(registry, contexts);
        l.onMessage(message(ConfigChangedMessage.EntityType.OUTPUT));
        verify(registry).invalidate();
        l.onMessage(message(ConfigChangedMessage.EntityType.GROUP));
        verify(contexts).invalidateAll();
        l.onMessage(message(ConfigChangedMessage.EntityType.SETTING));
        verify(registry, times(2)).invalidate();
        l.onMessage(message(ConfigChangedMessage.EntityType.FLOW));
        l.onMessage(new Message("{".getBytes(), new MessageProperties()));
        verify(contexts, times(2)).invalidateAll();
    }

    private Message message(ConfigChangedMessage.EntityType type) {
        ConfigChangedMessage m = ConfigChangedMessage.upsert(type, 1, 3, 1, clock);
        return new Message(MessageCodec.create().write(m), new MessageProperties());
    }
}
