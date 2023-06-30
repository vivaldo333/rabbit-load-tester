package org.ks.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.ks.MessageHelper;
import org.ks.dto.ProcessingResultDto;
import org.ks.dto.PromiseDto;
import org.ks.dto.RabbitConfigDto;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker implements Runnable, Closeable {

    private final Connection connection;
    private final Channel channel;
    private final LinkedBlockingQueue<PromiseDto> pendingForSendQueue;
    private final RabbitConfigDto rabbitConfig;
    private final MessageHelper messageHelper;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public Worker(Connection connection,
                  LinkedBlockingQueue<PromiseDto> pendingForSendQueue,
                  RabbitConfigDto rabbitConfig) {
        this.connection = connection;
        this.pendingForSendQueue = pendingForSendQueue;
        this.channel = createChannel();
        this.rabbitConfig = rabbitConfig;
        this.messageHelper = new MessageHelper();
        messageHelper.declareRabbitMqComponents(
                rabbitConfig.getExchangerName(), rabbitConfig.getQueueName(), rabbitConfig.getRoutingKey(), channel);
        System.out.println("Worker created");
    }

    @Override
    public void run() {
        System.out.println("Worker started to send");
        try {
            while (!isClosed.get() || !pendingForSendQueue.isEmpty()) {
                PromiseDto promise = null;
                try {
                    promise = pendingForSendQueue.take();
                    sendToChannel(promise.getMessage());
                    promise.getResult().complete(
                            new ProcessingResultDto(promise.getMessage(), true)
                    );
                } catch (Exception ex) {
                    Optional.ofNullable(promise).ifPresent(promiseResult ->
                            promiseResult.getResult().completeExceptionally(ex));
                }
            }
        } finally {
            System.out.println("Worker finished to send");
        }
    }

    @Override
    public void close() {
        isClosed.set(true);
        try {
            channel.close();
            connection.close();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendToChannel(String message) throws IOException {
        System.out.println("send message: " + message);
        var exchangeName = rabbitConfig.getExchangerName();
        var routingKey = rabbitConfig.getRoutingKey();
        var messageBodyBytes = getMessage(message);
        if (exchangeName != null) {
            channel.basicPublish(exchangeName,
                    routingKey,
                    messageHelper.getMessageProperties(),
                    messageBodyBytes);
        } else {
            channel.basicPublish("", rabbitConfig.getQueueName(), null, messageBodyBytes);
        }
        System.out.println(
                "Worker" + Thread.currentThread().getName() + " - sendToChannel - message: " + message);
    }

    private byte[] getMessage(String message) {
        try {
            return message.getBytes(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private Channel createChannel() {
        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
