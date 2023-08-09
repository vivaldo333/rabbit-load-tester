package org.ks.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer extends DefaultConsumer {

    private AtomicInteger countConsumedMessages = new AtomicInteger(0);
    public Consumer(Channel channel) {
        super(channel);
    }

    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               AMQP.BasicProperties properties,
                               byte[] body) throws IOException {
        //var message = new String(body);
        //System.out.println("Handled message: " + message);
        //System.out.println("is redelivered: " + envelope.isRedeliver());
        //var routingKey = envelope.getRoutingKey();
        //var contentType = properties.getContentType();
        long deliveryTag = envelope.getDeliveryTag();
        getChannel().basicAck(deliveryTag, false);
        var currentCountConsumedMessages = countConsumedMessages.incrementAndGet();
        System.out.println("Consumed message: " + currentCountConsumedMessages);
    }

    public int getCountConsumedMessages() {
        return countConsumedMessages.get();
    }
}
