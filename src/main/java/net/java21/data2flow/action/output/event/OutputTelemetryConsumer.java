package net.java21.data2flow.action.output.event;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import net.java21.data2flow.action.output.service.OutputEnqueueService;
import net.java21.data2flow.contracts.message.CanonicalTelemetry;
import net.java21.data2flow.contracts.message.MessageCodec;
import net.java21.data2flow.contracts.message.MessageFormatException;
import net.java21.data2flow.contracts.messaging.MessagingNames;
import net.java21.data2flow.contracts.messaging.SuperStreamSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 출력 연결용 {@code data2flow.telemetry} 소비자(DSC-04.01, BR-DSC-19). 소비자 그룹 {@code action-output}(로컬 {@code action-output-<이름>})은
 * 플로우(flow)·실시간 화면(core-live)과 따로라서 출력 대상이 느리거나 죽어도 수집·플로우가 늦어지지 않는다.
 * <ul>
 *   <li>Single Active Consumer, 파티션마다 작업 스레드 하나가 순서대로 최대 {@value #MAX_BATCH}건씩 묶어 대기열에 쌓는다</li>
 *   <li>쌓기가 커밋된 뒤에만 오프셋을 저장한다. 실패하면(core 응답 없음 등) 같은 묶음을 1초→30초 간격으로 다시 시도한다(유실 0, 멱등 쌓기)</li>
 *   <li>저장된 오프셋이 없으면 지금부터 읽는다(지난 데이터를 한꺼번에 외부로 보내지 않게)</li>
 *   <li>종료: 새 메시지를 받지 않고 처리 중인 묶음을 끝낸 뒤 닫는다(넘겨받은 쪽이 저장된 오프셋 다음부터)</li>
 * </ul>
 */
public class OutputTelemetryConsumer implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutputTelemetryConsumer.class);
    static final int MAX_QUEUED = 1000;
    static final int MAX_BATCH = 200;

    private final OutputStreamConnection connection;
    private final OutputEnqueueService enqueue;
    private final String groupName;
    private final MessageCodec codec = MessageCodec.create();
    private final Map<Integer, PartitionWorker> workers = new ConcurrentHashMap<>();
    private final AtomicInteger inFlight = new AtomicInteger();
    private volatile CountDownLatch stopSignal = new CountDownLatch(1);
    private volatile Consumer consumer;
    private volatile boolean running;
    private volatile boolean stopping;
    private Thread starter;

    public OutputTelemetryConsumer(OutputStreamConnection connection, OutputEnqueueService enqueue, String groupName) {
        this.connection = connection;
        this.enqueue = enqueue;
        this.groupName = groupName;
    }

    public String groupName() {
        return groupName;
    }

    @Override
    public void start() {
        running = true;
        stopping = false;
        stopSignal = new CountDownLatch(1);
        starter = Thread.ofVirtual().name("output-consumer-start").start(this::connect);
    }

    private void connect() {
        long delay = 1000;
        while (running && consumer == null) {
            try {
                // 생산자(pipeline)가 만들기 전에 구독하면 파티션 0개로 "성공"하므로 첫 파티션이 생길 때까지 기다린다
                if (!connection.environment().streamExists(SuperStreamSpec.TELEMETRY.partition(0))) {
                    throw new IllegalStateException(MessagingNames.STREAM_TELEMETRY + " Super Stream이 아직 없습니다(pipeline이 만듦)");
                }
                consumer = connection.environment().consumerBuilder()
                        .superStream(MessagingNames.STREAM_TELEMETRY)
                        .name(groupName)
                        .singleActiveConsumer()
                        .manualTrackingStrategy().builder()
                        .consumerUpdateListener(context -> {
                            worker(OutputStreamConnection.partitionIndex(context.stream())).newGeneration();
                            if (!context.isActive()) {
                                return null;
                            }
                            try {
                                return OffsetSpecification.offset(context.consumer().storedOffset() + 1);
                            } catch (NoOffsetException e) {
                                return OffsetSpecification.next();
                            }
                        })
                        .messageHandler(this::handle)
                        .build();
                log.info("data2flow.telemetry 출력 소비 시작(그룹 {})", groupName);
            } catch (RuntimeException e) {
                log.warn("출력 소비자를 열지 못했습니다(다시 시도): {}", e.getMessage());
                if (pause(delay)) {
                    return;
                }
                delay = Math.min(delay * 2, 30_000);
            }
        }
    }

    /** 종료 신호가 오면 true */
    private boolean pause(long millis) {
        try {
            return stopSignal.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    private PartitionWorker worker(int partition) {
        return workers.computeIfAbsent(partition, PartitionWorker::new);
    }

    private void handle(MessageHandler.Context context, Message message) {
        if (stopping) {
            return;
        }
        worker(OutputStreamConnection.partitionIndex(context.stream())).submit(context, message);
    }

    /** 묶음을 쌓고 커밋되면 마지막 오프셋 저장. 실패하면 같은 묶음을 다시 시도 */
    private void process(List<PartitionWorker.Entry> batch, java.util.function.IntSupplier generation, int gen) {
        if (batch.isEmpty()) {
            return;
        }
        List<CanonicalTelemetry> items = new ArrayList<>(batch.size());
        for (PartitionWorker.Entry e : batch) {
            try {
                items.add(codec.read(e.message().getBodyAsBinary(), CanonicalTelemetry.class));
            } catch (MessageFormatException ex) {
                log.warn("읽을 수 없는 텔레메트리를 건너뜁니다({} 오프셋 {}): {}", e.context().stream(), e.context().offset(), ex.getMessage());
            }
        }
        long delay = 1000;
        inFlight.incrementAndGet();
        try {
            while (!stopping && generation.getAsInt() == gen) {
                try {
                    if (!items.isEmpty()) {
                        enqueue.enqueue(items);
                    }
                    batch.getLast().context().storeOffset();
                    return;
                } catch (RuntimeException e) {
                    log.warn("출력 대기열 쌓기 실패(오프셋 저장 안 함, {}ms 뒤 다시): {}", delay, e.toString());
                    if (pause(delay)) {
                        return;
                    }
                    delay = Math.min(delay * 2, 30_000);
                }
            }
        } finally {
            inFlight.decrementAndGet();
        }
    }

    @Override
    public void stop() {
        stopping = true;
        stopSignal.countDown();
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        CountDownLatch idle = new CountDownLatch(1);
        while (inFlight.get() > 0 && System.nanoTime() < deadline) {
            try {
                idle.await(20, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Consumer c = consumer;
        consumer = null;
        workers.values().forEach(PartitionWorker::close);
        workers.clear();
        if (c != null) {
            try {
                c.close();
            } catch (RuntimeException e) {
                log.debug("소비자 닫기 실패: {}", e.getMessage());
            }
        }
        running = false;
        if (starter != null) {
            starter.interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** 웹 서버보다 나중에, DB·AMQP보다 먼저 멈춘다 */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 100;
    }

    public boolean isConsuming() {
        return consumer != null;
    }

    /** 파티션 하나의 순차 작업 스레드(가상 스레드). 파티션을 다른 인스턴스가 넘겨받으면(세대가 바뀌면) 남은 메시지는 버린다 */
    private final class PartitionWorker {
        record Entry(int generation, MessageHandler.Context context, Message message) {
        }

        private final LinkedBlockingQueue<Entry> queue = new LinkedBlockingQueue<>(MAX_QUEUED);
        private final AtomicInteger generation = new AtomicInteger();
        private final Thread thread;
        private volatile boolean closed;

        PartitionWorker(int partition) {
            this.thread = Thread.ofVirtual().name("output-partition-" + partition).start(this::loop);
        }

        void newGeneration() {
            generation.incrementAndGet();
        }

        void submit(MessageHandler.Context context, Message message) {
            try {
                queue.put(new Entry(generation.get(), context, message));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void loop() {
            while (!closed) {
                Entry first;
                try {
                    first = queue.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (first == null) {
                    continue;
                }
                List<Entry> batch = new ArrayList<>(MAX_BATCH);
                batch.add(first);
                queue.drainTo(batch, MAX_BATCH - 1);
                int current = generation.get();
                batch.removeIf(e -> e.generation() != current);
                try {
                    process(batch, generation::get, current);
                } catch (RuntimeException e) {
                    log.warn("출력 묶음 처리 실패: {}", e.toString());
                }
            }
        }

        void close() {
            closed = true;
            try {
                thread.join(Duration.ofSeconds(20));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            queue.clear();
        }
    }
}
