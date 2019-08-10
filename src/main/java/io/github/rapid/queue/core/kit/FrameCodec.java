package io.github.rapid.queue.core.kit;

import java.nio.ByteBuffer;
import java.util.LinkedList;

// MagicNumber(short) + PayloadLength(Int) + PayloadChecksum(Long) + PayloadBytes(byte[]) + Ending(byte: 127)
public final class FrameCodec {
    public final static byte ENDING_BYTE_VAL = 127;

    private final static int LEN_HEAD = Short.BYTES + Integer.BYTES + Long.BYTES;
    private final static int LEN_ENDING = Byte.BYTES;
    private final static int LEN_CHECK_SUM_ADD_ENDING = Long.BYTES + LEN_ENDING;
    private final static int LEN_HEAD_ADD_ENDING = LEN_HEAD + LEN_ENDING;
    private final static short MAGIC_NUMBER_V1 = 10;

    private int maxFrameLength;


    public FrameCodec(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
    }


    public void encodeWrite(ByteBuffer byteBuffer, byte[] payload) {
        long checksum;
        try (Checker checker = Checker.getDefaultChecker();) {
            checker.update(payload, 0, payload.length);
            checker.update(ENDING_BYTE_VAL);
            checksum = checker.getValue();
        }
        byteBuffer.putShort(MAGIC_NUMBER_V1)
                .putInt(payload.length)
                .putLong(checksum)
                .put(payload)
                .put(ENDING_BYTE_VAL);
    }

    public void decode(CircularBuffer circularBuffer, LinkedList<MessageFrame> messageFrames) {
        while (circularBuffer.getLength() >= FrameCodec.LEN_HEAD) {
            circularBuffer.markNextReadPos();
            short magicNumber = circularBuffer.getShort();
            int payloadSize = circularBuffer.getInt();
            if (magicNumber == FrameCodec.MAGIC_NUMBER_V1) {
                if (circularBuffer.getLength() < payloadSize + LEN_CHECK_SUM_ADD_ENDING) {
                    circularBuffer.resetNextReadPos2Mark();
                    break;
                } else {
                    long checksum0 = circularBuffer.getLong();
                    byte[] payload = circularBuffer.getNextBytes(payloadSize);
                    long checksum;
                    try (Checker checker = Checker.getDefaultChecker()) {
                        checker.update(payload, 0, payload.length);
                        checker.update(circularBuffer.getByte());
                        checksum = checker.getValue();
                    }
                    if (checksum != checksum0) {
                        throw new IllegalArgumentException("checksum error [checksum0：" + checksum + ", checksum:" + checksum + "]");
                    }
                    MessageFrame messageFrame = new MessageFrame(
                            frameLengthAddPayloadLen(payloadSize)
                            , payload
                    );
                    messageFrames.add(messageFrame);
                }
            } else {
                throw new IllegalArgumentException("Illegal MAGIC_NUMBER # " + magicNumber);
            }
        }
    }


    public int frameLengthAddPayloadLen(int payloadLen) {
        int frameLen = payloadLen + LEN_HEAD_ADD_ENDING;
        if (frameLen > maxFrameLength) {
            throw new IllegalArgumentException("frameLen:" + frameLen + ", maxFrameLength:" + maxFrameLength);
        }
        return frameLen;
    }
}
