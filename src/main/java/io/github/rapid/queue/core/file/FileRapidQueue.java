package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.RapidQueue;
import io.github.rapid.queue.core.RapidQueueMessage;
import io.github.rapid.queue.core.RapidQueueListener;
import io.github.rapid.queue.core.RapidQueueReader;
import io.github.rapid.queue.core.kit.SimpleLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;


class FileRapidQueue implements RapidQueue {

    private final StoreMessageHelper fileDateHelper;
    final FileMessageCircularCache circularCache;
    private volatile HashMap<Long, FileRapidQueueListener> listenerMap = new HashMap<>();
    private final SimpleLock GLOBAL_LOCK;
    private final long lockWaitTimeMillis;

    FileRapidQueue(SimpleLock GLOBAL_LOCK
            , long lockWaitTimeMillis
            , File dataDir
            , int maxFrameLength, int maxPageSize
            , int writerPerSize, int readerPerSize
            , int cachePageSize
    ) throws IOException {
        this.GLOBAL_LOCK = GLOBAL_LOCK;
        this.lockWaitTimeMillis = lockWaitTimeMillis;
        this.fileDateHelper = StoreMessageHelper.createOpened(dataDir, maxFrameLength, maxPageSize, writerPerSize, readerPerSize);
        this.circularCache = new FileMessageCircularCache(cachePageSize);
    }

    private final static int NOT_DURABLE_OFFSET = -1;

    private void lock() {
        GLOBAL_LOCK.lock();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean tryLock(long time) {
        return GLOBAL_LOCK.tryLock(time);
    }

    void unLock() {
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
                RapidQueueMessage message = new RapidQueueMessage(offset, body, true);
                circularCache.add(message);
                notifyListener(message);
                return offset;
            } else {
                RapidQueueMessage message = new RapidQueueMessage(NOT_DURABLE_OFFSET, body, false);
                notifyListener(message);
                return NOT_DURABLE_OFFSET;
            }
        } finally {
            unLock();
        }
    }

    private void notifyListener(RapidQueueMessage message) {
        for (FileRapidQueueListener listener : listenerMap.values()) {
            listener.onMessage(message);
        }
    }

    @Override
    public RapidQueueListener newMessageListener() {
        checkStopped();
        return new FileRapidQueueListener(this);
    }

    @Override
    public RapidQueueReader readSnapshot(@Nullable Long offset) throws IOException {
        checkStopped();
        StoreMessageReader dataReader = fileDateHelper.readSnapshot(offset);
        return new RapidQueueReader() {
            @Override
            @Nonnull
            public Iterator<RapidQueueMessage> iterator() {
                return dataReader.iterator();
            }

            @Override
            public void close() throws IOException {
                dataReader.close();
            }
        };
    }

    synchronized void removeTailListener(Long listenerId) {
        checkStopped();
        HashMap<Long, FileRapidQueueListener> newMap = new HashMap<>(listenerMap);
        newMap.remove(listenerId);
        listenerMap = newMap;
    }

    synchronized void putListener(Long listenerId, FileRapidQueueListener listener) {
        checkStopped();
        HashMap<Long, FileRapidQueueListener> newMap = new HashMap<>(listenerMap);
        newMap.put(listenerId, listener);
        listenerMap = newMap;
    }

    private void checkStopped() {
        if (stopped.get()) {
            throw new IllegalArgumentException("rapid queue stopped");
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
