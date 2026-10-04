package net.java21.data2flow.action.support;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import java.time.Duration;

/**
 * 통합 시험 인프라: Testcontainers PostgreSQL 18 + RabbitMQ 3.13(Quorum 큐·Stream). JVM에 하나씩만 띄워 모든 IT가 함께 쓴다.
 * 실제 s3·s4 인프라와 공용 브로커에는 붙지 않는다(CLAUDE.md §5).
 */
public final class TestInfrastructure {

    public static final int STREAM_PORT = 5552;

    @SuppressWarnings("resource")
    public static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine").withDatabaseName("data2flow");
    @SuppressWarnings("resource")
    public static final GenericContainer<?> RABBIT = new GenericContainer<>("rabbitmq:3.13-management")
            .withExposedPorts(5672, STREAM_PORT)
            // 출력 연결(DSC-04.01) 스트림 IT가 data2flow.telemetry Super Stream을 쓴다
            .withCopyToContainer(Transferable.of("[rabbitmq_management,rabbitmq_stream]."), "/etc/rabbitmq/enabled_plugins")
            .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1).withStartupTimeout(Duration.ofMinutes(3)));

    static {
        POSTGRES.start();
        RABBIT.start();
    }

    private TestInfrastructure() {
    }

    public static int amqpPort() {
        return RABBIT.getMappedPort(5672);
    }

    public static int streamPort() {
        return RABBIT.getMappedPort(STREAM_PORT);
    }
}
