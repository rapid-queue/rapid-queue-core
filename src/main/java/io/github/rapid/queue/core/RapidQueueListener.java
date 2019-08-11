package io.github.rapid.queue.core;

import javax.annotation.Nullable;
import java.io.IOException;

public interface RapidQueueListener {
    void start(@Nullable Long offsetId, RapidQueueCallback callback) throws IOException;

    void stop();

    boolean statusActive();

    boolean statusStop();
}
