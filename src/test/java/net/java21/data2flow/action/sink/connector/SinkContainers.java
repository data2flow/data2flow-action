package net.java21.data2flow.action.sink.connector;

import net.java21.data2flow.action.common.Json;
import net.java21.data2flow.contracts.secret.Secret;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Sink 대상 컨테이너(Testcontainers, 시험 JVM마다 쓰는 것만 하나씩 띄움). 실제 s3·s4·외부 저장소에는 붙지 않는다(CLAUDE.md §5).
 * MySQL은 MySQLContainer 대신 GenericContainer(드라이버 대기 없음)이고, 중단·복구 시험(TC-FLW-084)을 위해 호스트 포트를 고정해 멈췄다 다시 켜도 주소가 같게 한다.
 */
public final class SinkContainers {

    public static final String INFLUX_TOKEN = "data2flow-test-token";
    public static final String INFLUX_ORG = "data2flow";
    public static final String INFLUX_BUCKET = "sink";

    private SinkContainers() {
    }

    /** PostgreSQL 18 대상 */
    public static final class Postgres {
        @SuppressWarnings("resource")
        public static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:18-alpine").withDatabaseName("target");

        static {
            CONTAINER.start();
        }

        private Postgres() {
        }

        public static SinkConnection connection(long id, long org) {
            return new SinkConnection(id, org, "POSTGRESQL", Map.of("host", CONTAINER.getHost(), "port", CONTAINER.getMappedPort(5432),
                    "database", "target", "username", CONTAINER.getUsername()), Map.of("password", Secret.of(CONTAINER.getPassword())), 1);
        }
    }

    /** MySQL 8 대상(드라이버는 MariaDB Connector/J) */
    public static final class MySql {
        public static final int HOST_PORT = freePort();
        @SuppressWarnings("resource")
        public static final GenericContainer<?> CONTAINER = new GenericContainer<>("mysql:8.4")
                .withEnv("MYSQL_DATABASE", "target").withEnv("MYSQL_USER", "sink").withEnv("MYSQL_PASSWORD", "sink-pass")
                .withEnv("MYSQL_ROOT_PASSWORD", "root-pass").withExposedPorts(3306)
                // MySQL Connector/J(GPL)가 없으므로 JDBC 대기 대신 로그로 기다린다(초기화용 임시 서버는 port: 0)
                .waitingFor(Wait.forLogMessage(".*ready for connections.*port: 3306.*", 1).withStartupTimeout(Duration.ofMinutes(3)))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                        new com.github.dockerjava.api.model.PortBinding(com.github.dockerjava.api.model.Ports.Binding.bindPort(HOST_PORT),
                                new com.github.dockerjava.api.model.ExposedPort(3306))));

        static {
            CONTAINER.start();
        }

        private MySql() {
        }

        public static SinkConnection connection(long id, long org) {
            return new SinkConnection(id, org, "MYSQL", config(), Map.of("password", Secret.of("sink-pass")), 1);
        }

        public static Map<String, Object> config() {
            return Map.of("host", CONTAINER.getHost(), "port", HOST_PORT, "database", "target", "username", "sink");
        }

        public static String jdbcUrl() {
            return "jdbc:mariadb://" + CONTAINER.getHost() + ":" + HOST_PORT + "/target?allowPublicKeyRetrieval=true&sslMode=disable";
        }
    }

    /** InfluxDB 2 대상 */
    public static final class Influx {
        @SuppressWarnings("resource")
        public static final GenericContainer<?> CONTAINER = new GenericContainer<>("influxdb:2.7-alpine")
                .withExposedPorts(8086)
                .withEnv("DOCKER_INFLUXDB_INIT_MODE", "setup")
                .withEnv("DOCKER_INFLUXDB_INIT_USERNAME", "admin")
                .withEnv("DOCKER_INFLUXDB_INIT_PASSWORD", "admin-password")
                .withEnv("DOCKER_INFLUXDB_INIT_ORG", INFLUX_ORG)
                .withEnv("DOCKER_INFLUXDB_INIT_BUCKET", INFLUX_BUCKET)
                .withEnv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", INFLUX_TOKEN)
                .waitingFor(Wait.forHttp("/ping").forStatusCode(204).withStartupTimeout(Duration.ofMinutes(2)));

        static {
            CONTAINER.start();
            // 클래스 초기화 중이라 같은 스레드에서 확인한다(다른 스레드는 초기화 잠금에 막힘). 초기 설정 모드는 influxd를 한 번 띄웠다 다시 켠다: 버킷이 보일 때까지 기다린다
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(90)).pollInSameThread().ignoreExceptions().until(() -> {
                HttpRequest req = HttpRequest.newBuilder(URI.create(url() + "/api/v2/buckets?name=" + INFLUX_BUCKET))
                        .header("Authorization", "Token " + INFLUX_TOKEN).GET().build();
                HttpResponse<String> res = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
                return res.statusCode() == 200 && Json.MAPPER.readTree(res.body()).path("buckets").size() > 0;
            });
        }

        private Influx() {
        }

        public static String url() {
            return "http://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(8086);
        }

        public static SinkConnection connection(long id, long org) {
            return new SinkConnection(id, org, "INFLUXDB", config(), Map.of("token", Secret.of(INFLUX_TOKEN)), 1);
        }

        public static Map<String, Object> config() {
            return Map.of("url", url(), "org", INFLUX_ORG, "bucket", INFLUX_BUCKET);
        }

        /** measurement의 temperature 점 수(Flux count) */
        public static long count(String measurement) throws IOException, InterruptedException {
            String flux = "from(bucket: \"" + INFLUX_BUCKET + "\") |> range(start: 0) |> filter(fn: (r) => r._measurement == \""
                    + measurement + "\" and r._field == \"temperature\") |> group() |> count()";
            HttpRequest req = HttpRequest.newBuilder(URI.create(url() + "/api/v2/query?org=" + INFLUX_ORG))
                    .header("Authorization", "Token " + INFLUX_TOKEN).header("Content-Type", "application/json")
                    .header("Accept", "application/csv")
                    .POST(HttpRequest.BodyPublishers.ofString(Json.write(Map.of("query", flux, "type", "flux")))).build();
            String csv = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString()).body();
            long total = 0;
            for (String line : csv.split("\r?\n")) {
                String[] cells = line.split(",");
                if (line.isBlank() || line.contains("_value") || cells.length == 0) {
                    continue;
                }
                total += Long.parseLong(cells[cells.length - 1].trim());
            }
            return total;
        }
    }

    /**
     * 고정 호스트 포트. 임시 포트 범위(49152~)에서 고르면 같은 JVM이 나중에 여는 서버(시험 Tomcat 등)와 겹칠 수 있어서 20000~29999에서
     * 비어 있는 포트를 고른다.
     */
    private static int freePort() {
        java.util.Random random = new java.util.Random();
        for (int i = 0; i < 200; i++) {
            int port = 20000 + random.nextInt(10000);
            try (ServerSocket socket = new ServerSocket(port)) {
                return port;
            } catch (IOException e) {
                // 사용 중: 다른 포트
            }
        }
        throw new IllegalStateException("빈 포트를 찾지 못했습니다");
    }
}
