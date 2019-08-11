package io.github.rapid.queue.core.kit;

final public class SimpleLockNoLock implements SimpleLock {
    @Override
    public void lock() {

    }

    @Override
    public boolean tryLock(long time) {
        return true;
    }

    @Override
    public void unLock() {

    }
}
