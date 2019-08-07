package io.github.rapid.queue.core;

import javax.annotation.Nullable;
import java.io.IOException;

public interface SequenceListener {
    void start(@Nullable Long offsetId, MessageCallback messageCallback) throws IOException;

    void stop();

    boolean statusActive();

    boolean statusStop();
}
