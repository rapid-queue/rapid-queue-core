package io.github.rapid.queue.core.kit;

public interface SimpleLock {
    void lock();

    boolean tryLock(long time);

    void unLock();
}
