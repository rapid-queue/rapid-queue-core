package io.github.rapid.queue.core.kit;


import java.util.zip.Checksum;

public final class Checker implements AutoCloseable, Checksum {
    private static final ThreadLocal<Checker> CHECKER_THREAD_LOCAL = ThreadLocal.withInitial(() -> new Checker(new PureJavaCrc32C()));
    private final Checksum checksum;

    private Checker(Checksum checksum) {
        this.checksum = checksum;
    }

    @Override
    public void close() {
        checksum.reset();
    }


    static Checker getDefaultChecker() {
        return CHECKER_THREAD_LOCAL.get();
    }

    @Override
    public void update(int b) {
        checksum.update(b);
    }

    @Override
    public void update(byte[] b, int off, int len) {
        checksum.update(b, off, len);
    }

    @Override
    public long getValue() {
        return checksum.getValue();
    }

    @Override
    public void reset() {
        checksum.reset();
    }
}
