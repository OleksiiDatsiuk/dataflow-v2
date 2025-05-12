package org.arpha;

import lombok.SneakyThrows;

public class ProducerRunner {

    @SneakyThrows
    public static void main(String[] args) {

        Runnable producerTask = () -> {
            try {
                new Producer("localhost", 9999).run();
            } catch (InterruptedException e) {
                throw new IllegalArgumentException(e);
            }
        };

        Thread producerThread = new Thread(producerTask);

        Thread.sleep(3000);
        producerThread.start();
    }
}
