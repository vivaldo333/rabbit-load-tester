package org.ks.consumer.concurrent;

import com.rabbitmq.client.Connection;
import org.graalvm.collections.Pair;
import org.ks.MessageHelper;
import org.ks.dto.RabbitConfigDto;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.nonNull;

public final class ChannelHelper {
    public static final int PREFETCH_MESSAGE_COUNT = 1000;
    public static final boolean AUTO_ACK_FLAG = false;
    public static final String CONSUMER_TAG = "event-hub-load-test-consumer";

    public List<Future<Pair<ConsumerTask, String>>> initializeConsumers(
            Connection connection, int countConsumers, ExecutorService executorService) {
        try {
            System.out.println("Thread pool initialized with count: " + countConsumers);
            var consumerTasks = getConsumerTasks(connection, countConsumers, executorService);
            System.out.println("Created consumer tasks: " + consumerTasks.size());
            return executorService.invokeAll(consumerTasks);
        } catch (InterruptedException e) {
            System.out.println("Channel error: " + e.getMessage());
            return Collections.emptyList();
        }

    }

    public void closeConsumers(List<Future<Pair<ConsumerTask, String>>> futureTasks) {
        futureTasks.stream().filter(Future::isDone)
                .map(ChannelHelper::getConsumerTask)
                .filter(futureTask -> nonNull(futureTask.getRight()))
                .peek(futureTask -> System.out.println("completed consumer tag: " + futureTask.getRight()))
                .map(Pair::getLeft)
                .forEach(ConsumerTask::closeConsumer);
    }

    private List<ConsumerTask> getConsumerTasks(Connection connection, int countConsumers, ExecutorService executorService) {
        return IntStream.range(0, countConsumers)
                .mapToObj(idx -> getConsumer(connection, idx, executorService))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<ConsumerTask> getConsumer(Connection connection, int consumerTagIndex, ExecutorService executorService) {
        var messageHelper = new MessageHelper();
        var rabbitConfig = new RabbitConfigDto();
        try {
            //TODO (tsymbvol 2023-08-18): should be autoclosable
            var channel = connection.createChannel();
            channel.basicQos(0, PREFETCH_MESSAGE_COUNT, false);
            messageHelper.declareRabbitMqComponents(
                    rabbitConfig.getExchangerName(), rabbitConfig.getQueueName(), rabbitConfig.getRoutingKey(), channel);
            System.out.println("exchanger: " + rabbitConfig.getExchangerName() + " queue: " + rabbitConfig.getQueueName() + " were declared");
            var consumerTask = new ConsumerTask(channel, consumerTagIndex, rabbitConfig.getQueueName());
            System.out.println("Consumer task created with tag: " + consumerTask.getConsumerTag());
            return Optional.of(consumerTask);
        } catch (IOException e) {
            System.out.println("Channel error: " + e.getMessage());
            return Optional.empty();
        }
    }

    private static Pair<ConsumerTask, String> getConsumerTask(Future<Pair<ConsumerTask, String>> futureTask) {
        try {
            return futureTask.get();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println("Future task: " + e.getMessage());
            return Pair.empty();
        }
    }

}
