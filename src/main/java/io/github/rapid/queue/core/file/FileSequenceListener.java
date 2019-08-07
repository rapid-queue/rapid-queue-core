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
    //这里一定要过滤的就是前后顺序，不保证数据的完整性，这里会尽力保证完整性但是完整性和顺序性的校验是应用做的，不是这里做的

    //读文件，直到读到比内存中的数据要大的时候，再从内存页中读取，内存页中读取的时候会锁住写入的数据，这样就可以安全了
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
                        //这里不能先缓存再处理，因为写入的线程会写入新的数据
                        if (stop.get()) return;
                        onMessage(message);
                        if (stop.get()) return;
                    }
                    fileSequencer.putListener(listenerId, this);
                    active.set(true);
                    shouldReadFile = false;
                } else if (circularReaderStatus.equals(FileSequencerCircularCache.ReaderStatus.EMPTY)) {
                    //如果page中是空的那么就是从启动开始没有数据进入，那么必须要读文件，如果读过文件了，还是空的那么就可以放行了
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
            //这不是一个持久化的信息
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
