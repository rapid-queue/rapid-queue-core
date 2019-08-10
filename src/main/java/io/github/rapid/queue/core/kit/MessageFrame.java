package io.github.rapid.queue.core.kit;


public final class MessageFrame {
    private final int frameLength;
    private final byte[] payload;

    MessageFrame(int frameLength, byte[] payload) {
        this.frameLength = frameLength;
        this.payload = payload;
    }

    public byte[] getPayload() {
        return payload;
    }


    public int getFrameLength() {
        return frameLength;
    }
}
