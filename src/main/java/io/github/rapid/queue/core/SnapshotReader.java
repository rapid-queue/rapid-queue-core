package io.github.rapid.queue.core;

import java.io.IOException;

public interface SnapshotReader extends AutoCloseable, Iterable<EventMessage> {
    @Override
    void close() throws IOException;
}
