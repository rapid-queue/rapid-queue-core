package io.github.rapid.queue.core.kit;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SpinLock implements SimpleLock {

    /**
     * 锁持有线程, null表示锁未被任何线程持有
     */
    private final AtomicReference<Thread> owner = new AtomicReference<>();

    /**
     * owner持有锁次数
     */
    private int holdCount;

    @Override
    public void lock() {
        final AtomicReference<Thread> owner = this.owner;

        final Thread current = Thread.currentThread();
        if (owner.get() == current) { // 当前线程已持有锁, 增加持有计数即可
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
            // 响应中断
            if (current.isInterrupted()) {
                current.interrupt();
                return false;
            }
            // 判断是否超时
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
        // 持有多少次, 就必须释放多少次
        if (--holdCount == 0) {
            owner.set(null);
        }
    }

}

