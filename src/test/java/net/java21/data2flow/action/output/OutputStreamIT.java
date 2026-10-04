package net.java21.data2flow.action.output;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import net.java21.data2flow.action.output.event.OutputTelemetryConsumer;
import net.java21.data2flow.action.support.TestInfrastructure;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * 출력 연결 스트림 소비(DSC-04.01, BR-DSC-19): {@code data2flow.telemetry}(Super Stream, 파티션 3)를 소비자 그룹 {@code action-output}으로
 * 따로 읽어 대기열에 쌓는다. 대상이 죽어 있어도(발송 실패) 소비·쌓기는 늦어지지 않는다(AT-DSC-10.2 "수집 지연 증가 없음").
 */
@TestPropertySource(properties = {"data2flow.action.output.consumer-enabled=true", "data2flow.action.output.stream-fixed-address=true"})
class OutputStreamIT extends OutputSupport {

    private static final MessageCodec CODEC = MessageCodec.create();

    @DynamicPropertySource
    static void stream(DynamicPropertyRegistry registry) {
        registry.add("data2flow.action.output.stream-port", TestInfrastructure::streamPort);
        try (Environment env = environment()) {
            env.streamCreator().name(SuperStreamSpec.TELEMETRY.name()).maxAge(SuperStreamSpec.TELEMETRY.maxAge())
                    .superStream().partitions(3).creator().create();
        } catch (StreamException e) {
            // 이미 있음
        }
    }

    @Autowired
    OutputTelemetryConsumer consumer;

    static Environment environment() {
        String host = TestInfrastructure.RABBIT.getHost();
        int port = TestInfrastructure.streamPort();
        return Environment.builder().host(host).port(port).username("guest").password("guest").virtualHost("/")
                .addressResolver(a -> new Address(host, port)).build();
    }

    @Test
    @DisplayName("[DSC-04.01][AT-DSC-10.2][BR-DSC-19] data2flow.telemetry를 그룹 action-output으로 읽어 연결마다 쌓는다(대상 장애와 무관, 멱등)")
    void consumesTelemetryIntoQueue() throws Exception {
        setConnections(List.of(OutputFixtures.webhook(71, "http://127.0.0.1:9/down", Map.of("metrics", List.of("co2")), Map.of(), Map.of())));
        assertThat(consumer.groupName()).isEqualTo("action-output");
        await().atMost(Duration.ofSeconds(60)).until(consumer::isConsuming);
        try (Environment env = environment()) {
            Producer producer = env.producerBuilder().superStream(MessagingNames.STREAM_TELEMETRY)
                    .routing(m -> m.getApplicationProperties().get("routingKey").toString()).producerBuilder().build();
            // 소비자가 파티션을 받은 뒤(지금부터 읽음) 보낸 것만 쌓인다 — 받을 때까지 하나씩 보낸다
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                publish(producer, OutputFixtures.am107(clock.instant(), 500));
                return count("SELECT count(*) FROM data2flow_action.output_deliveries WHERE output_id = 71") > 0;
            });
            long before = count("SELECT count(*) FROM data2flow_action.output_deliveries WHERE output_id = 71");
            for (int i = 0; i < 5; i++) {
                publish(producer, OutputFixtures.am107(clock.instant(), 600 + i));
            }
            await().atMost(Duration.ofSeconds(30))
                    .until(() -> count("SELECT count(*) FROM data2flow_action.output_deliveries WHERE output_id = 71") >= before + 5);
            producer.close();
        }
        assertThat(count("SELECT count(*) FROM data2flow_action.output_deliveries WHERE output_id = 71 AND part = ''")).isPositive();
        assertThat(count("SELECT count(*) FROM data2flow_action.output_deliveries WHERE status <> 'PENDING'")).isZero();
    }

    private static void publish(Producer producer, CanonicalTelemetry t) throws Exception {
        CompletableFuture<Boolean> confirmed = new CompletableFuture<>();
        producer.send(producer.messageBuilder().properties().messageId(t.messageId().toString()).messageBuilder()
                        .applicationProperties().entry("routingKey", t.routingKey()).messageBuilder()
                        .addData(CODEC.write(t)).build(),
                status -> confirmed.complete(status.isConfirmed()));
        assertThat(confirmed.get(10, TimeUnit.SECONDS)).isTrue();
    }
}
