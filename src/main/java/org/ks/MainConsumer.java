package org.ks;

import com.rabbitmq.client.Address;
import org.ks.consumer.Consumer;
import org.ks.dto.RabbitConfigDto;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MainConsumer {

    public static void main(String[] args) {
        var rabbitConfig = new RabbitConfigDto();
        var messageHelper = new MessageHelper();
        var connectionFactory = messageHelper.getConnectionFactory(rabbitConfig.getHost(), rabbitConfig.getPort());
        var autoAck = false;
        var consumerTag = "event-hub-load-test-consumer";
        var messageCount = 10000;
        var prefetchMessageCount = 1;
        var hosts = messageHelper.getHosts();
        hosts = new Address[]{
                new Address("event-hub-test1", 5672)
                ,
                new Address("event-hub-test2", 5672)
                ,
                new Address("event-hub-test3", 5672)
        };

        try (var connection = connectionFactory.newConnection(hosts);
             var channel = connection.createChannel()) {
            // Per consumer limit - Java will receive a maximum of "prefetchMessageCount" unacknowledged messages at once

            // prefetchSize – maximum amount of content (measured in octets) that the server will deliver, 0 if unlimited
            // prefetchCount – maximum number of messages that the server will deliver, 0 if unlimited
            // global – true if the settings should be applied to the entire channel rather than each consumer
            channel.basicQos(0, prefetchMessageCount, false);
            // No limit for this consumer
            //.basicQos(0);

            messageHelper.declareRabbitMqComponents(
                    rabbitConfig.getExchangerName(), rabbitConfig.getQueueName(), rabbitConfig.getRoutingKey(), channel);
            var consumer = new Consumer(channel);
            consumerTag = channel.basicConsume(rabbitConfig.getQueueName(), autoAck, consumerTag, consumer);
            System.out.println("Consumer tag: " + consumerTag);

            while (consumer.getCountConsumedMessages() != messageCount) {
                Thread.sleep(200);
            }

            channel.basicCancel(consumerTag);
            System.out.println("Total count consumed messages: " + consumer.getCountConsumedMessages());
        } catch (IOException | TimeoutException | InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
