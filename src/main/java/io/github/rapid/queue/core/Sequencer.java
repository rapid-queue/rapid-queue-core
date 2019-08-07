package io.github.rapid.queue.core;


import io.github.rapid.queue.core.file.FileSequenceBuilder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

public interface Sequencer extends AutoCloseable {

    long append(byte[] body, boolean durable) throws IOException;

    SequenceListener newMessageListener();

    SnapshotReader readSnapshot(@Nullable Long offsetId) throws IOException;

    void close() throws IOException;

    static FileSequenceBuilder createFileSequencerBuilder(File dataDir) {
        return new FileSequenceBuilder(dataDir);
    }
}
