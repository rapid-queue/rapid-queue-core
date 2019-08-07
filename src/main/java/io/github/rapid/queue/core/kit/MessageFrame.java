package io.github.rapid.queue.core.kit;


public final class MessageFrame {
    //这个完整帧的长度
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
