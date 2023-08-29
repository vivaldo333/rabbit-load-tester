package org.ks;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.ks.dto.ProcessingResultDto;
import org.ks.dto.PromiseDto;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MainSender {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Main - START: " + LocalDateTime.now());
        args = new String[]{
                "1000000",
                "1",
                "event.hub.load.test.q",
                "event.hub.exchanger",
                "event.hub.load.test.router"};

        if (args != null && args.length > 0) {
            var messageQuantity = Integer.parseInt(args[0]);
            var parallelism = Integer.parseInt(args[1]);
            var queueName = args[2];
            var exchangeName = args[3];
            var routingKey = args[4];

            LinkedBlockingDeque<PromiseDto> requestQueue = new LinkedBlockingDeque<>(messageQuantity);
            var loader = new Loader(requestQueue);
            var executor = Executors.newFixedThreadPool(parallelism);
            var availableParallelismSemaphore = new Semaphore(parallelism);
            var messageHelper = new MessageHelper();
            var factory = messageHelper.getConnectionFactory();


            for (int i = 0; i < messageQuantity; i++) {
                var message = "message_" + i;
                loader.send(message);

            }
            try (Connection connection = factory.newConnection("event-hub_load_test");
                 Channel channel = connection.createChannel()) {
                messageHelper.declareRabbitMqComponents(exchangeName, queueName, routingKey, channel);

                var workers = IntStream.range(0, parallelism)
                        .mapToObj(idx -> new Sender(
                                availableParallelismSemaphore, channel, requestQueue, queueName, exchangeName, routingKey))
                        .collect(Collectors.toList());

                var results = executor.invokeAll(workers);
                results.stream()
                        .map(MainSender::getProcessingResult)
                        .flatMap(List::stream)
                        .forEach(res -> {
                            if (res.isSuccess()) {
                                loader.getCountSuccessRequest().incrementAndGet();
                            } else {
                                loader.getCountFailedRequest().incrementAndGet();
                            }
                        });

                System.out.println("Count Request: " + loader.getCountIncomingRequest().get());
                System.out.println("Count Success Request: " + loader.getCountSuccessRequest().get());
                System.out.println("Count Failed Request: " + loader.getCountFailedRequest().get());

                awaitTerminationAfterShutdown(executor);
            } catch (IOException | TimeoutException e) {
                System.out.println("Error: " + e.getMessage());
            }


        }
        System.out.println("Main - END: " + LocalDateTime.now());
    }

    private static List<ProcessingResultDto> getProcessingResult(Future<List<ProcessingResultDto>> res) {
        try {
            return res.get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Error: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    public static void awaitTerminationAfterShutdown(ExecutorService threadPool) {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException ex) {
            threadPool.shutdownNow();
            //Thread.currentThread().interrupt();
        }
    }
}