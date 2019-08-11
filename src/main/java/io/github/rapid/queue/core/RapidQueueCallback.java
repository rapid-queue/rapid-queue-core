package io.github.rapid.queue.core;

public interface RapidQueueCallback {
    void onMessage(RapidQueueMessage rapidQueueMessage);
}
