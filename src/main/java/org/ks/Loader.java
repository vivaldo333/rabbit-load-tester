package org.ks;

import org.ks.dto.ProcessingResultDto;
import org.ks.dto.PromiseDto;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class Loader {


    private AtomicInteger countIncomingRequest = new AtomicInteger(0);
    private AtomicInteger countSuccessRequest = new AtomicInteger(0);
    private AtomicInteger countFailedRequest = new AtomicInteger(0);

    private LinkedBlockingDeque<PromiseDto> requestQueue;
    private MessageMapper messageMapper;


    public Loader(LinkedBlockingDeque<PromiseDto> requestQueue) {
        this.requestQueue = requestQueue;
        this.messageMapper = new MessageMapper();
    }

    public CompletableFuture<ProcessingResultDto> send(String message) throws InterruptedException {
        var promise = messageMapper.map(message);
        requestQueue.put(promise);
        countIncomingRequest.incrementAndGet();
        return promise.getResult();
    }

    public AtomicInteger getCountIncomingRequest() {
        return countIncomingRequest;
    }

    public AtomicInteger getCountSuccessRequest() {
        return countSuccessRequest;
    }

    public AtomicInteger getCountFailedRequest() {
        return countFailedRequest;
    }
}
