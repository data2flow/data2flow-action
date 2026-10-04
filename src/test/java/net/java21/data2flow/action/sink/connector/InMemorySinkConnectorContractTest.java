package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.contracts.secret.Secret;

import java.util.Map;

/** 키트를 메모리 참조 구현으로 실행(키트 자체가 맞는지 빠르게 확인, TC-FLW-087) */
class InMemorySinkConnectorContractTest extends SinkConnectorContractTest {

    private final InMemorySinkConnector connector = new InMemorySinkConnector();

    @Override
    protected SinkConnector connector() {
        return connector;
    }

    @Override
    protected SinkConnection connection() {
        return new SinkConnection(1, 1, "MEMORY", Map.of("host", "memory"), Map.of(), 1);
    }

    @Override
    protected SinkConnection withWrongCredentials() {
        return new SinkConnection(1, 1, "MEMORY", Map.of("host", "memory"), Map.of("password", Secret.of("wrong")), 1);
    }

    @Override
    protected SinkConnection withUnknownHost() {
        return new SinkConnection(1, 1, "MEMORY", Map.of("host", "no-such-host.invalid"), Map.of(), 1);
    }

    @Override
    protected SinkConnection withClosedPort() {
        return new SinkConnection(1, 1, "MEMORY", Map.of("host", "down"), Map.of(), 1);
    }

    @Override
    protected long count(String target) {
        return connector.count(target);
    }
}
