package io.github.rapid.queue.core;

import java.io.IOException;

public interface RapidQueueReader extends AutoCloseable, Iterable<RapidQueueMessage> {
    @Override
    void close() throws IOException;
}
