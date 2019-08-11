package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.RapidQueue;
import io.github.rapid.queue.core.kit.SimpleLockJUC;
import io.github.rapid.queue.core.kit.SimpleLock;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

final public class RapidQueueBuilder {
    private final File dataDir;

    private SimpleLock lock = new SimpleLockJUC();
    private long lockWaitTimeMillis = TimeUnit.SECONDS.toMillis(3);
    private int maxFrameLength = Math.toIntExact((FileUtils.ONE_KB * 4));
    private int pageSize = Math.toIntExact((FileUtils.ONE_GB + 512 * FileUtils.ONE_MB));
    private int writeSize = Math.toIntExact(FileUtils.ONE_MB * 16);
    private int readSize = Math.toIntExact(FileUtils.ONE_MB);
    private int cachePageSize = 1024;

    public RapidQueueBuilder(File dataDir) {
        this.dataDir = dataDir;
    }

    public RapidQueueBuilder setLock(SimpleLock lock) {
        this.lock = lock;
        return this;
    }

    public RapidQueueBuilder setLockWaitTimeMillis(long lockWaitTimeMillis) {
        if (lockWaitTimeMillis < 0) {
            throw new IllegalArgumentException("lockWaitTimeMillis should >= 0");
        }
        this.lockWaitTimeMillis = lockWaitTimeMillis;
        return this;
    }

    public RapidQueueBuilder setMaxFrameLength(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
        return this;
    }


    public RapidQueueBuilder setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public RapidQueueBuilder setWriteSize(int writeSize) {
        this.writeSize = writeSize;
        return this;
    }

    public RapidQueueBuilder setReadSize(int readSize) {
        this.readSize = readSize;
        return this;
    }

    public RapidQueueBuilder setCachePageSize(int cachePageSize) {
        this.cachePageSize = cachePageSize;
        return this;
    }

    public RapidQueue build() throws IOException {
        return new FileRapidQueue(lock
                , lockWaitTimeMillis
                , dataDir
                , maxFrameLength
                , pageSize
                , writeSize
                , readSize
                , cachePageSize
        );
    }
}
