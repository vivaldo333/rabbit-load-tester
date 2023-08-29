package org.ks.consumer.concurrent;

import org.ks.MessageHelper;
import org.ks.dto.RabbitConfigDto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.ks.MainSender.awaitTerminationAfterShutdown;

public class MainMultithreadingConsumer {
    public static final int MESSAGE_COUNT = 1_00_000;
    public static final Map<String, String> EXPECTED_MESSAGES = new HashMap<>(MESSAGE_COUNT);
    public static final AtomicInteger CONSUMED_COUNT = new AtomicInteger(0);

    public static void main(String[] args) {
        var rabbitConfig = new RabbitConfigDto();
        var messageHelper = new MessageHelper();
        var channelHelper = new ChannelHelper();
        var connectionFactory = messageHelper.getConnectionFactory(rabbitConfig.getHost(), rabbitConfig.getPort());
        var countParallelConsumers = 100;
        var hosts = messageHelper.getHosts();
        var executorService = Executors.newFixedThreadPool(countParallelConsumers);

        IntStream.range(0, MESSAGE_COUNT + 1).forEach(idx -> {
            var expectedMessage = "" + idx;
            EXPECTED_MESSAGES.put(expectedMessage, expectedMessage);
        });
        System.out.println("Initialized expected messages: " + EXPECTED_MESSAGES.size());
        try (var connection = connectionFactory.newConnection(hosts)) {
            var futureTasks = channelHelper.initializeConsumers(connection, countParallelConsumers, executorService);
            System.out.println("Future consumer task created: " + futureTasks.size());
            while (CONSUMED_COUNT.get() <= MESSAGE_COUNT) {
                Thread.sleep(100);
            }
            System.out.println("Global count consumed messages: " + CONSUMED_COUNT.get());
            channelHelper.closeConsumers(futureTasks);
            awaitTerminationAfterShutdown(executorService);
            System.out.println("Parallel tasks were closed");
        } catch (IOException | TimeoutException | InterruptedException e) {
            System.out.println("Connection error: " + e.getMessage());
        }
    }
}
