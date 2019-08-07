package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.EventMessage;
import io.github.rapid.queue.core.SequenceListener;
import io.github.rapid.queue.core.Sequencer;
import io.github.rapid.queue.core.SnapshotReader;
import io.github.rapid.queue.core.kit.SimpleLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;


final public class FileSequencer implements Sequencer, SimpleLock {

    private final StoreMessageHelper fileDateHelper;
    final FileSequencerCircularCache circularCache;
    private volatile HashMap<String, FileSequenceListener> listenerMap = new HashMap<>();
    private final SimpleLock GLOBAL_LOCK;
    private final long lockWaitTimeMillis;

    FileSequencer(SimpleLock GLOBAL_LOCK
            , long lockWaitTimeMillis
            , File dataDir
            , int maxFrameLength, int maxPageSize
            , int writerPerSize, int readerPerSize
            , int cachePageSize
    ) throws IOException {
        this.GLOBAL_LOCK = GLOBAL_LOCK;
        this.lockWaitTimeMillis = lockWaitTimeMillis;
        this.fileDateHelper = StoreMessageHelper.createOpened(dataDir, maxFrameLength, maxPageSize, writerPerSize, readerPerSize);
        this.circularCache = new FileSequencerCircularCache(cachePageSize);
    }

    private final static int NOT_DURABLE_OFFSET = -1;

    @Override
    public void lock() {
        GLOBAL_LOCK.lock();
    }

    @Override
    public boolean tryLock(long time) {
        return GLOBAL_LOCK.tryLock(time);
    }

    @Override
    public void unLock() {
        GLOBAL_LOCK.unLock();
    }

    @Override
    public long append(byte[] body, boolean durable) throws IOException {
        checkStopped();
        if (lockWaitTimeMillis == 0) {
            lock();
        } else {
            if (!tryLock(lockWaitTimeMillis)) {
                throw new IOException("write wait time out");
            }
        }
        try {
            if (durable) {
                long offset = fileDateHelper.writeAppend(body);
                EventMessage message = new EventMessage(offset, body, true);
                circularCache.add(message);
                notifyListener(message);
                return offset;
            } else {
                EventMessage message = new EventMessage(NOT_DURABLE_OFFSET, body, false);
                notifyListener(message);
                return NOT_DURABLE_OFFSET;
            }
        } finally {
            unLock();
        }
    }

    private void notifyListener(EventMessage message) {
        for (FileSequenceListener listener : listenerMap.values()) {
            listener.onMessage(message);
        }
    }

    @Override
    public SequenceListener newMessageListener() {
        checkStopped();
        return new FileSequenceListener(this);
    }

    @Override
    public SnapshotReader readSnapshot(@Nullable Long offset) throws IOException {
        checkStopped();
        StoreMessageReader dataReader = fileDateHelper.readSnapshot(offset);
        return new SnapshotReader() {
            @Override
            @Nonnull
            public Iterator<EventMessage> iterator() {
                return dataReader.iterator();
            }

            @Override
            public void close() throws IOException {
                dataReader.close();
            }
        };
    }

    synchronized void removeTailListener(String listenerId) {
        checkStopped();
        HashMap<String, FileSequenceListener> newMap = new HashMap<>(listenerMap);
        newMap.remove(listenerId);
        listenerMap = newMap;
    }

    synchronized void putListener(String listenerId, FileSequenceListener listener) {
        checkStopped();
        HashMap<String, FileSequenceListener> newMap = new HashMap<>(listenerMap);
        newMap.put(listenerId, listener);
        listenerMap = newMap;
    }

    private void checkStopped() {
        if (stopped.get()) {
            throw new IllegalArgumentException("sequencer stopped");
        }
    }

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    @Override
    public void close() {
        if (stopped.compareAndSet(false, true)) {
            fileDateHelper.close();
        }
    }

}
