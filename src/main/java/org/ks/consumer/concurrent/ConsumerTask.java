package org.ks.consumer.concurrent;

import com.rabbitmq.client.Channel;
import org.graalvm.collections.Pair;
import org.ks.consumer.Consumer;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.ks.consumer.concurrent.ChannelHelper.AUTO_ACK_FLAG;
import static org.ks.consumer.concurrent.ChannelHelper.CONSUMER_TAG;

public class ConsumerTask implements Callable<Pair<ConsumerTask, String>> {
    private final Channel channel;
    private final int consumerTagIndex;
    private final String queueName;
    private final Consumer consumer;

    public ConsumerTask(Channel channel, int consumerTagIndex, String queueName) {
        this.channel = channel;
        this.consumerTagIndex = consumerTagIndex;
        this.queueName = queueName;
        this.consumer = new Consumer(channel);
    }

    @Override
    public Pair<ConsumerTask, String> call() throws Exception {
        var consumerTag = channel.basicConsume(queueName, AUTO_ACK_FLAG, getConsumerTag(), consumer);
        System.out.println("Consumer tag: " + consumerTag);
        return Pair.create(this, consumerTag);
    }

    public void closeConsumer() {
        try {
            System.out.println(String.format("Total count consumed messages: %s by consumer: %s", consumer.getCountConsumedMessages(), this.getConsumerTag()));
            var channel = this.getChannel();
            channel.basicCancel(this.getConsumerTag());
            channel.close();
        } catch (IOException | TimeoutException e) {
            System.out.println("Error close channel: " + e.getMessage());
        }
    }

    public String getConsumerTag() {
        return CONSUMER_TAG + "_" + this.consumerTagIndex;
    }

    public Channel getChannel() {
        return channel;
    }

    public Consumer getConsumer() {
        return consumer;
    }
}
