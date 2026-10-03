package net.java21.data2flow.action.actuation.service;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@code wait=ack|applied}(API-ACT-01, 최대 10초) 대기 신호. 이 파드에서 상태가 바뀌면 바로 깨우고, 다른 파드가 처리한 경우를 위해
 * 호출 쪽은 짧은 간격으로 DB를 다시 읽는다({@code Thread.sleep} 대신 신호 대기).
 */
public class CommandWaiter {

    private final Map<UUID, CompletableFuture<Void>> waiting = new ConcurrentHashMap<>();

    /** 신호가 오거나 {@code max}가 지날 때까지 기다린다 */
    public void awaitSignal(UUID commandId, Duration max) {
        CompletableFuture<Void> f = waiting.computeIfAbsent(commandId, k -> new CompletableFuture<>());
        try {
            f.get(Math.max(1, max.toMillis()), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException ignored) {
            // 시간이 지나면 호출 쪽이 DB를 다시 읽는다
        } finally {
            waiting.remove(commandId, f);
        }
    }

    /** 상태가 바뀌었다 */
    public void signal(UUID commandId) {
        CompletableFuture<Void> f = waiting.remove(commandId);
        if (f != null) {
            f.complete(null);
        }
    }
}
