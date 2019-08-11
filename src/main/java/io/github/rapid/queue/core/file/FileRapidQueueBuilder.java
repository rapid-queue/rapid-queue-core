package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.JUCLock;
import io.github.rapid.queue.core.kit.SimpleLock;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

final public class FileRapidQueueBuilder {
    private final File dataDir;

    private SimpleLock lock = new JUCLock();
    private long lockWaitTimeMillis = TimeUnit.SECONDS.toMillis(3);
    private int maxFrameLength = Math.toIntExact((FileUtils.ONE_KB * 4));
    private int pageSize = Math.toIntExact((FileUtils.ONE_GB + 512 * FileUtils.ONE_MB));
    private int writeSize = Math.toIntExact(FileUtils.ONE_MB * 16);
    private int readSize = Math.toIntExact(FileUtils.ONE_MB);
    private int cachePageSize = 1024;

    public FileRapidQueueBuilder(File dataDir) {
        this.dataDir = dataDir;
    }

    public FileRapidQueueBuilder setLock(SimpleLock lock) {
        this.lock = lock;
        return this;
    }

    public FileRapidQueueBuilder setLockWaitTimeMillis(long lockWaitTimeMillis) {
        if (lockWaitTimeMillis < 0) {
            throw new IllegalArgumentException("lockWaitTimeMillis should >= 0");
        }
        this.lockWaitTimeMillis = lockWaitTimeMillis;
        return this;
    }

    public FileRapidQueueBuilder setMaxFrameLength(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
        return this;
    }


    public FileRapidQueueBuilder setPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public FileRapidQueueBuilder setWriteSize(int writeSize) {
        this.writeSize = writeSize;
        return this;
    }

    public FileRapidQueueBuilder setReadSize(int readSize) {
        this.readSize = readSize;
        return this;
    }

    public FileRapidQueueBuilder setCachePageSize(int cachePageSize) {
        this.cachePageSize = cachePageSize;
        return this;
    }

    public FileRapidQueue build() throws IOException {
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
