package org.ks;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageHelper {
    public AMQP.BasicProperties getMessageProperties() {
        // https://www.rabbitmq.com/amqp-0-9-1-reference.html
        return new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
                .headers(getMessageHeaders())
                .deliveryMode(2) //Non-persistent (1) or persistent (2)
                .priority(0) //Message priority, 0 to 9
                //.expiration("61000") //TTL ms
                .userId("admin")
                .appId("event-hub")
                .build();
    }

    public Map<String, Object> getMessageHeaders() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("x-delay", 15000);
        return headers;
    }


    public void declareRabbitMqComponents(String exchangeName, String queueName, String routingKey, Channel channel) {
        try {
            declareDelayedExchanger(exchangeName, channel);
            declareQuorumQueue(queueName, channel);
            channel.queueBind(queueName, exchangeName, routingKey);
        } catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void declareDelayedExchanger(String exchangeName, Channel channel) throws IOException {
        Map<String, Object> args = new HashMap<>();
        args.put("x-delayed-type", "direct");
        channel.exchangeDeclare(exchangeName, "x-delayed-message", true, false, args);
    }

    public void declareQuorumQueue(String queueName, Channel channel) throws IOException {
        channel.queueDeclare(queueName, true, false, false, getQuorumQueueAttributes());
    }

    public ConnectionFactory getConnectionFactory() {
        var factory = new ConnectionFactory();
        factory.setHost("event-hub-test1");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin_pass");
        factory.setVirtualHost(ConnectionFactory.DEFAULT_VHOST);
        //factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");
        return factory;
    }

    public ConnectionFactory getConnectionFactory(String host, int port) {
        var factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername("admin");
        factory.setPassword("admin_pass");
        factory.setVirtualHost(ConnectionFactory.DEFAULT_VHOST);
        //factory.setUri("amqp://userName:password@hostName:portNumber/virtualHost");
        return factory;
    }

    public Address[] getHosts() {
        return new Address[]{
                //new Address("event-hub-test1", 5672),
                //new Address("event-hub-test2", 5672),
                new Address("event-hub-test3", 5672)
        };
    }

    private Map<String, Object> getQuorumQueueAttributes() {
        // https://www.rabbitmq.com/quorum-queues.html
        Map<String, Object> args = new HashMap<>();
        //x-max-length	Number
        //x-max-length-bytes	Number
        //x-overflow	"drop-head" or "reject-publish"
        //x-expires	Number (milliseconds) Message expiration settings, supported only by classic (not quorum) queue
        //x-dead-letter-exchange	String
        //x-dead-letter-routing-key	String
        //x-max-in-memory-length	Number
        //x-max-in-memory-bytes	Number
        //x-delivery-limit	Number //increment header "x-delivery-count", supported only by quorum queue

        //args.put("x-max-priority", 10); //supported only by classic (not quorum) queue
        //maximum number of unconfirmed messages a channel accepts before entering flow
        //args.put("rabbit.quorum_commands_soft_limit", 512); //default 32
        args.put("x-queue-type", "quorum"); //default "classic"
        args.put("x-quorum-initial-group-size", 3); //default 3 in advanced.config
        //https://www.rabbitmq.com/quorum-queues.html#replication-factor
        //client-local: Pick the node the client that declares the queue is connected to. This is the default value.
        //balanced: If there are overall less than 1000 queues (classic queues, quorum queues, and streams), pick the node hosting the minimum number of quorum queue leaders. If there are overall more than 1000 queues, pick a random node.
        args.put("x-queue-leader-locator", "client-local"); //default "client-local" , other "balanced"
        //specifies maximum value of retries. After that, message is deadlettered or deleted
        //rabbitmq will make 1 attempt to deliver a message before deleting it
        args.put("x-delivery-limit", 1); //default not set

        return args;
    }
}
