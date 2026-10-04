package net.java21.data2flow.action.sink.connector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 이 커넥터가 계약 키트(TC-FLW-087 {@code SinkConnectorContractTest})를 통과했다는 표시. 값은 키트를 상속한 시험 클래스의 단순 이름이고,
 * 시험({@code SinkConnectorRegistryTest})이 그 클래스가 실제로 키트를 상속하는지 확인한다. 표시가 없는 커넥터는 등록하지 않는다.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface SinkConnectorVerified {

    /** 계약 시험 클래스 이름(예: PostgresSinkConnectorContractTest) */
    String value();
}
