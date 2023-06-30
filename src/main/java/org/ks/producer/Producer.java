package org.ks.producer;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.ks.MessageHelper;
import org.ks.dto.ProcessingResultDto;
import org.ks.dto.PromiseDto;
import org.ks.dto.RabbitConfigDto;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Producer implements Closeable {
    private final ConnectionFactory connectionFactory;
    private final List<Worker> workers;
    private final LinkedBlockingQueue<PromiseDto> requestQueue;
    private final int parallelism;
    private final ExecutorService executorService;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final MessageHelper messageHelper = new MessageHelper();
    private final RabbitConfigDto rabbitConfig;

    public Producer(RabbitConfigDto rabbitConfig, int parallelism) {
        this.rabbitConfig = rabbitConfig;
        this.parallelism = parallelism;
        this.requestQueue = new LinkedBlockingQueue<>();
        this.connectionFactory = messageHelper.getConnectionFactory(rabbitConfig.getHost(), rabbitConfig.getPort());
        this.executorService = Executors.newFixedThreadPool(parallelism);
        this.workers = generateWorkers();
        submitWorkers(this.workers);
    }

    public CompletableFuture<ProcessingResultDto> send(String message) {
        CompletableFuture<ProcessingResultDto> futureResult = new CompletableFuture<>();
        futureResult.orTimeout(6L, TimeUnit.SECONDS);
        if (isClosed.get()) {
            futureResult.completeExceptionally(new RuntimeException("Closed Producer"));
        } else {
            var promise = new PromiseDto(message, futureResult);
            try {
                requestQueue.put(promise);
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
                futureResult.completeExceptionally(e);
            }
        }

        return futureResult;
    }

    private List<Worker> generateWorkers() {
        return IntStream.range(0, parallelism)
                .mapToObj(idx -> new Worker(getConnection(), requestQueue, rabbitConfig))
                .collect(Collectors.toList());
    }

    private Connection getConnection() {
        try {
            return connectionFactory.newConnection(messageHelper.getHosts());
            //return connectionFactory.newConnection("event-hub_load_test_other");
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void executeWorker(Worker worker) {
        executorService.execute(worker);
    }

    private void submitWorkers(List<Worker> workers) {
        workers.forEach(this::executeWorker);
    }

    @Override
    public void close() throws IOException {
        isClosed.set(true);
        workers.forEach(Worker::close);
        awaitTerminationAfterShutdown(executorService);
    }

    private static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(20, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
        }
    }
}
