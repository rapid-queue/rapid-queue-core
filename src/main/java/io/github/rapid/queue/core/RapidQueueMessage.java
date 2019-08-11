package io.github.rapid.queue.core;


public final class RapidQueueMessage {
    private final long offset;
    private final byte[] body;
    private final boolean durable;

    public RapidQueueMessage(long offset, byte[] body, boolean durable) {
        this.offset = offset;
        this.body = body;
        this.durable = durable;
    }

    public long getOffset() {
        return offset;
    }

    public byte[] getBody() {
        return body;
    }

    public boolean isDurable() {
        return durable;
    }

    @Override
    public String toString() {
        return "RapidQueueMessage{" +
                "offset=" + offset +
                ", durable=" + durable +
                '}';
    }
}
