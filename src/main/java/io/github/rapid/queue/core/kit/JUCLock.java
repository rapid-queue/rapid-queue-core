package io.github.rapid.queue.core.kit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class JUCLock implements SimpleLock {
    private final Lock lock = new ReentrantLock();


    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public boolean tryLock(long timeMillis) {
        try {
            return lock.tryLock(timeMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    @Override
    public void unLock() {
        lock.unlock();
    }
}
