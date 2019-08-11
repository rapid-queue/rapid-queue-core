package io.github.rapid.queue.core;


import io.github.rapid.queue.core.file.FileRapidQueueBuilder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

public interface RapidQueue extends AutoCloseable {

    long append(byte[] body, boolean durable) throws IOException;

    RapidQueueListener newMessageListener();

    RapidQueueReader readSnapshot(@Nullable Long offsetId) throws IOException;

    void close() throws IOException;

    static FileRapidQueueBuilder createFileSequencerBuilder(File dataDir) {
        return new FileRapidQueueBuilder(dataDir);
    }
}
