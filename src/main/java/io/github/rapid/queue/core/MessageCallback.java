package io.github.rapid.queue.core;

public interface MessageCallback {
    void onMessage(EventMessage eventMessage);
}
