package org.ks.consumer.concurrent;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MainMultithreadingConsumerExample {
    private static final Logger logger = LoggerFactory.getLogger(MainMultithreadingConsumerExample.class);
    private static final String QUEUE_NAME = "hello";


    public static void main(String[] args) throws IOException, TimeoutException {
        int threadNumber = 2;
        final ExecutorService threadPool = new ThreadPoolExecutor(threadNumber, threadNumber,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        logger.info(" [*] Waiting for messages. To exit press CTRL+C");

        registerConsumer(channel, 500, threadPool);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Invoking shutdown hook...");
                logger.info("Shutting down thread pool...");
                threadPool.shutdown();
                try {
                    while (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) ;
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for termination");
                }
                logger.info("Thread pool shut down.");
                logger.info("Done with shutdown hook.");
            }
        });
    }

    private static void registerConsumer(final Channel channel, final int timeout, final ExecutorService threadPool)
            throws IOException {
        channel.exchangeDeclare(QUEUE_NAME, "fanout");
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "");

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       final byte[] body) throws IOException {
                try {
                    logger.info(String.format("Received (channel %d) %s", channel.getChannelNumber(), new String(body)));

                    threadPool.submit(new Runnable() {
                        public void run() {
                            try {
                                Thread.sleep(timeout);
                                logger.info(String.format("Processed %s", new String(body)));
                            } catch (InterruptedException e) {
                                logger.warn(String.format("Interrupted %s", new String(body)));
                            }
                        }
                    });
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        };

        channel.basicConsume(QUEUE_NAME, true /* auto-ack */, consumer);
    }
}
