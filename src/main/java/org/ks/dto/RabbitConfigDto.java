package org.ks.dto;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitConfigDto {
    private String host = "event-hub-test1";
    private int port = 5672;
    private String username = "admin";
    private String password = "admin_pass";
    private String virtualHost = ConnectionFactory.DEFAULT_VHOST;
    private String queueName = "event.hub.load.test.other.q";
    private String exchangerName = "event.hub.other.exchanger";
    private String routingKey = "event.hub.load.test.other.router";

    public RabbitConfigDto() {
    }

    public RabbitConfigDto(String host, int port, String username, String password, String virtualHost, String queueName, String exchangerName, String routingKey) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.queueName = queueName;
        this.exchangerName = exchangerName;
        this.routingKey = routingKey;
    }

    public RabbitConfigDto(String host, int port, String virtualHost, String queueName, String exchangerName, String routingKey) {
        this.host = host;
        this.port = port;
        this.virtualHost = virtualHost;
        this.queueName = queueName;
        this.exchangerName = exchangerName;
        this.routingKey = routingKey;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getVirtualHost() {
        return virtualHost;
    }

    public void setVirtualHost(String virtualHost) {
        this.virtualHost = virtualHost;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExchangerName() {
        return exchangerName;
    }

    public void setExchangerName(String exchangerName) {
        this.exchangerName = exchangerName;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
}
