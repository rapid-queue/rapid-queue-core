package io.github.rapid.queue.core.file;


class FrameMessage {
    private final int frameLength;
    private final byte[] payload;

    FrameMessage(int frameLength, byte[] payload) {
        this.frameLength = frameLength;
        this.payload = payload;
    }

    byte[] getPayload() {
        return payload;
    }


    int getFrameLength() {
        return frameLength;
    }
}
