package org.ks;

import java.io.IOException;

public class MainProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        var parallelism = 1;
        var messageCount = 1_000_000;
        var loader = new GlobalLoader();
        loader.load(parallelism, messageCount);
    }

    //loading 10-20-50-100 threads
    //10-100-1000-10000 000 messages

    //publish 10 millions with x-delay
    //consumer takes messages
    //switch off node 1
    //calculate that app consumed from node 2, node 3 all messages with delay
    //switch on node 1
}

