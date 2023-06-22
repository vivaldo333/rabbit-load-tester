package org.ks;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.ks.dto.ProcessingResultDto;
import org.ks.dto.PromiseDto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Sender implements Callable<List<ProcessingResultDto>> {

    private Semaphore semaphore;
    private Channel channel;
    private LinkedBlockingDeque<PromiseDto> requestQueue;
    private String exchangeName;
    private String queueName;
    private String routingKey;
    private MessageMapper messageMapper;
    private MessageHelper messageHelper;

    public Sender(Semaphore semaphore,
                  Channel channel,
                  LinkedBlockingDeque<PromiseDto> requestQueue,
                  String queueName,
                  String exchangeName,
                  String routingKey) {
        this.semaphore = semaphore;
        this.channel = channel;
        this.requestQueue = requestQueue;
        this.queueName = queueName;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
        this.messageMapper = new MessageMapper();
        this.messageHelper = new MessageHelper();
    }

    public void send(String message) throws IOException {
        var messageBodyBytes = message.getBytes("UTF-8");
        if (exchangeName != null && !exchangeName.isEmpty()) {
            channel.basicPublish(exchangeName,
                    routingKey,
                    messageHelper.getMessageProperties(),
                    messageBodyBytes);
        } else {
            channel.basicPublish("", this.queueName, null, messageBodyBytes);
        }
        System.out.println("Sender - sent - message: " + message);
    }

    @Override
    public List<ProcessingResultDto> call() throws Exception {
        List<ProcessingResultDto> processingResults = new ArrayList<>();

        while (requestQueue != null && !requestQueue.isEmpty()) {
            var isSuccessResult = new AtomicBoolean(false);

            if (tryLock()) {
                var promiseResultOption = getPromiseMessage();
                try {
                    if (promiseResultOption.isPresent()) {
                        var promiseResult = promiseResultOption.get();
                        send(promiseResult.getMessage());
                        isSuccessResult.set(true);
                    }
                } catch (IOException | RuntimeException ex) {
                    System.out.println("Error");
                } finally {
                    if (promiseResultOption.isPresent()) {
                        var promise = promiseResultOption.get();
                        var processResult = messageMapper.map(promise.getMessage(), isSuccessResult.get());
                        processingResults.add(processResult);
                        promise.getResult().complete(processResult);
                    }
                    semaphore.release();
                }
            }
        }
        return processingResults;
    }





    private Optional<PromiseDto> getPromiseMessage() {
        try {
            return Optional.of(requestQueue.take());
        } catch (InterruptedException e) {
            System.out.println("Error:" + e.getMessage());
        }
        return Optional.empty();
    }

    private boolean tryLock() {
        try {
            if (semaphore.tryAcquire(5, TimeUnit.SECONDS)) {
                return true;
            }
        } catch (InterruptedException e) {
            System.out.println("Error: " + e.getMessage());
        }
        return false;
    }
}
