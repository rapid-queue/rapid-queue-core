package io.github.rapid.queue.core;



public final class EventMessage {
    private final long offset;
    private final byte[] body;
    private final boolean durable;

    public EventMessage(long offset, byte[] body, boolean durable) {
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
        return "EventMessage{" +
                "offset=" + offset +
                ", durable=" + durable +
                '}';
    }
}
