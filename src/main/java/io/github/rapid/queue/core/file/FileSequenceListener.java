package io.github.rapid.queue.core.file;


import io.github.rapid.queue.core.EventMessage;
import io.github.rapid.queue.core.MessageCallback;
import io.github.rapid.queue.core.SequenceListener;
import io.github.rapid.queue.core.SnapshotReader;
import io.github.rapid.queue.core.kit.UUIDKit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

final class FileSequenceListener implements SequenceListener {
    private final static Logger logger = LoggerFactory.getLogger(FileSequenceListener.class);

    private final String listenerId;
    private final FileSequencer fileSequencer;
    //
    private volatile Long lastOffsetId;
    private volatile MessageCallback messageCallback;

    FileSequenceListener(FileSequencer fileSequencer) {
        this.listenerId = UUIDKit.randomUUID();
        this.fileSequencer = fileSequencer;
    }

    private final Executor executor = Executors.newFixedThreadPool(10);

    @Override
    public void start(@Nullable Long offset, MessageCallback messageCallback) {
        Objects.requireNonNull(messageCallback, "messageListener can not null");
        this.messageCallback = messageCallback;
        this.lastOffsetId = offset;
        executor.execute(() -> {
            try {
                this.readExistingMessageActiveListener();
            } catch (Exception e) {
                logger.warn("FileSequenceListener readExistingMessageActiveListener error:", e);
                this.stop();
            }
        });
    }


    private void readExistingMessageActiveListener() throws IOException {
        boolean shouldReadFile = false;
        do {
            try {
                if (!fileSequencer.tryLock(3000)) {
                    throw new IllegalArgumentException("reader wait time out");
                }
                FileSequencerCircularCache.FullReader circularPageFullReader = fileSequencer.circularCache.createReader(lastOffsetId);
                FileSequencerCircularCache.ReaderStatus circularReaderStatus = circularPageFullReader.getStatus();
                if (circularReaderStatus.equals(FileSequencerCircularCache.ReaderStatus.GREATER)) {
                    fileSequencer.putListener(listenerId, this);
                    shouldReadFile = false;
                } else if (circularReaderStatus.equals(FileSequencerCircularCache.ReaderStatus.WITHIN)) {
                    for (EventMessage message : circularPageFullReader) {
                        if (stop.get()) return;
                        onMessage(message);
                        if (stop.get()) return;
                    }
                    fileSequencer.putListener(listenerId, this);
                    active.set(true);
                    shouldReadFile = false;
                } else if (circularReaderStatus.equals(FileSequencerCircularCache.ReaderStatus.EMPTY)) {
                    if (!shouldReadFile) {
                        shouldReadFile = true;
                    } else {
                        fileSequencer.putListener(listenerId, this);
                        active.set(true);
                        shouldReadFile = false;
                    }
                } else {
                    shouldReadFile = true;
                }
            } finally {
                fileSequencer.unLock();
            }

            if (shouldReadFile) {
                try (SnapshotReader reader = fileSequencer.readSnapshot(lastOffsetId)) {
                    for (EventMessage message : reader) {
                        if (stop.get()) return;
                        onMessage(message);
                        if (stop.get()) return;
                    }
                }
            }
        } while (shouldReadFile);
    }


    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicBoolean active = new AtomicBoolean(false);

    @Override
    public void stop() {
        stop.set(true);
        active.set(false);
        fileSequencer.removeTailListener(listenerId);
    }

    @Override
    public boolean statusActive() {
        return active.get();
    }

    public boolean statusStop() {
        return stop.get();
    }

    private boolean firstMessage = true;

    void onMessage(EventMessage message) {
        if (stop.get()) {
            return;
        }
        if (!message.isDurable()) {
            messageCallback.onMessage(message);
        } else {
            try {
                if (firstMessage && lastOffsetId == null) {
                    lastOffsetId = message.getOffset();
                }
                int i = StoreBase.compareOffset(message.getOffset(), lastOffsetId);
                if (i >= 0) {
                    if (i == 0) {
                        if (firstMessage) {
                            messageCallback.onMessage(message);
                            firstMessage = false;
                            lastOffsetId = message.getOffset();
                        }
                    } else {
                        messageCallback.onMessage(message);
                        lastOffsetId = message.getOffset();
                    }
                }
            } catch (Exception e) {
                logger.warn("FileSequenceListener error: ", e);
                stop();
            }
        }
    }
}
