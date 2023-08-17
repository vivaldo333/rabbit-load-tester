package org.ks;

import org.ks.dto.RabbitConfigDto;
import org.ks.producer.Producer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class GlobalLoader {

    private final Semaphore semaphore = new Semaphore(10000);
    private final AtomicInteger countSuccess = new AtomicInteger(0);
    private final AtomicInteger countUnsuccess = new AtomicInteger(0);


    void load(int parallelism, int messageCount) throws InterruptedException, IOException {
        var countDownLatch = new CountDownLatch(messageCount);
        var rabbitConfig = new RabbitConfigDto();

        var producer = new Producer(rabbitConfig, parallelism);
        var start = LocalDateTime.now();
        System.out.println("Start: " + start);
        for (int i = 0; i < messageCount; i++) {
            try {
                semaphore.acquire();
                var message = /*"test_message_"*/ "" + i;
                producer.send(message)
                        .thenAccept(result -> {
                            semaphore.release();
                            countDownLatch.countDown();
                            countSuccess.incrementAndGet();
                        })
                        .exceptionally(ex -> {
                            semaphore.release();
                            countDownLatch.countDown();
                            countUnsuccess.incrementAndGet();
                            return null;
                        });
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
        }

        countDownLatch.await();
        producer.close();
        var endDate = LocalDateTime.now();
        System.out.println("End: " + endDate + " - duration: " + ChronoUnit.SECONDS.between(start, endDate));
    }
}
