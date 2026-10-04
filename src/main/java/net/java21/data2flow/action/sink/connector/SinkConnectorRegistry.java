package net.java21.data2flow.action.sink.connector;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * 등록된 Sink 커넥터(종류 → 구현, FLW-04.05). 계약 키트를 통과했다고 표시({@link SinkConnectorVerified})한 구현만 받는다.
 * 같은 종류가 둘이면 기동을 막는다.
 */
public class SinkConnectorRegistry {

    private final Map<String, SinkConnector> byType = new LinkedHashMap<>();

    public SinkConnectorRegistry(List<SinkConnector> connectors) {
        for (SinkConnector c : connectors) {
            register(c);
        }
    }

    private void register(SinkConnector connector) {
        if (!connector.getClass().isAnnotationPresent(SinkConnectorVerified.class)) {
            throw new IllegalStateException("계약 키트(SinkConnectorContractTest)를 통과하지 않은 Sink 커넥터는 등록할 수 없습니다: "
                    + connector.getClass().getName());
        }
        String type = connector.type().toUpperCase(Locale.ROOT);
        if (byType.putIfAbsent(type, connector) != null) {
            throw new IllegalStateException("같은 종류의 Sink 커넥터가 둘입니다: " + type);
        }
    }

    public Optional<SinkConnector> find(String type) {
        return type == null ? Optional.empty() : Optional.ofNullable(byType.get(type.toUpperCase(Locale.ROOT)));
    }

    public List<String> types() {
        return List.copyOf(byType.keySet());
    }

    /** 연결이 바뀌면 모든 커넥터의 그 연결 풀을 닫는다 */
    public void release(long connectionId) {
        byType.values().forEach(c -> c.release(connectionId));
    }

    public void closeAll() {
        byType.values().forEach(SinkConnector::close);
    }
}
