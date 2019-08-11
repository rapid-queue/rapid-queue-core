package io.github.rapid.queue.core.file;


import io.github.rapid.queue.core.RapidQueueCallback;
import io.github.rapid.queue.core.RapidQueueListener;
import io.github.rapid.queue.core.RapidQueueMessage;
import io.github.rapid.queue.core.RapidQueueReader;
import io.github.rapid.queue.core.kit.UUIDKit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

final class FileRapidQueueListener implements RapidQueueListener {
    private final static Logger logger = LoggerFactory.getLogger(FileRapidQueueListener.class);

    private final String listenerId;
    private final FileRapidQueue fileRapidQueue;
    //
    private volatile Long lastOffsetId;
    private volatile RapidQueueCallback messageCallback;

    FileRapidQueueListener(FileRapidQueue fileRapidQueue) {
        this.listenerId = UUIDKit.randomUUID();
        this.fileRapidQueue = fileRapidQueue;
    }

    private final Executor executor = Executors.newFixedThreadPool(10);

    @Override
    public void start(@Nullable Long offset, RapidQueueCallback messageCallback) {
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
                if (!fileRapidQueue.tryLock(3000)) {
                    throw new IllegalArgumentException("reader wait time out");
                }
                FileMessageCircularCache.FullReader circularPageFullReader = fileRapidQueue.circularCache.createReader(lastOffsetId);
                FileMessageCircularCache.ReaderStatus circularReaderStatus = circularPageFullReader.getStatus();
                if (circularReaderStatus.equals(FileMessageCircularCache.ReaderStatus.GREATER)) {
                    fileRapidQueue.putListener(listenerId, this);
                    shouldReadFile = false;
                } else if (circularReaderStatus.equals(FileMessageCircularCache.ReaderStatus.WITHIN)) {
                    for (RapidQueueMessage message : circularPageFullReader) {
                        if (stop.get()) return;
                        onMessage(message);
                        if (stop.get()) return;
                    }
                    fileRapidQueue.putListener(listenerId, this);
                    active.set(true);
                    shouldReadFile = false;
                } else if (circularReaderStatus.equals(FileMessageCircularCache.ReaderStatus.EMPTY)) {
                    if (!shouldReadFile) {
                        shouldReadFile = true;
                    } else {
                        fileRapidQueue.putListener(listenerId, this);
                        active.set(true);
                        shouldReadFile = false;
                    }
                } else {
                    shouldReadFile = true;
                }
            } finally {
                fileRapidQueue.unLock();
            }

            if (shouldReadFile) {
                try (RapidQueueReader reader = fileRapidQueue.readSnapshot(lastOffsetId)) {
                    for (RapidQueueMessage message : reader) {
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
        fileRapidQueue.removeTailListener(listenerId);
    }

    @Override
    public boolean statusActive() {
        return active.get();
    }

    public boolean statusStop() {
        return stop.get();
    }

    private boolean firstMessage = true;

    void onMessage(RapidQueueMessage message) {
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
