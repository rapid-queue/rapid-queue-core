package io.github.rapid.queue.core.kit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

final public class SimpleLockSpin implements SimpleLock {

    private final AtomicReference<Thread> owner = new AtomicReference<>();

    private int holdCount;

    @Override
    public void lock() {
        final AtomicReference<Thread> owner = this.owner;

        final Thread current = Thread.currentThread();
        if (owner.get() == current) {
            ++holdCount;
            return;
        }

        //noinspection StatementWithEmptyBody
        while (!owner.compareAndSet(null, current)) {
        }

        holdCount = 1;
    }


    @Override
    public boolean tryLock(long timeMillis) {
        final AtomicReference<Thread> owner = this.owner;
        final Thread current = Thread.currentThread();
        if (owner.get() == current) {
            ++holdCount;
            return true;
        }

        final long start = System.nanoTime();
        final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeMillis);
        while (!owner.compareAndSet(null, current)) {
            if (current.isInterrupted()) {
                current.interrupt();
                return false;
            }
            long elapsed = System.nanoTime() - start;
            if (elapsed >= timeoutNanos) {
                return false;
            }
        }

        holdCount = 1;
        return true;
    }

    @Override
    public void unLock() {
        final AtomicReference<Thread> owner = this.owner;
        final Thread current = Thread.currentThread();

        if (owner.get() != current) {
            throw new IllegalMonitorStateException();
        }
        if (--holdCount == 0) {
            owner.set(null);
        }
    }

}

