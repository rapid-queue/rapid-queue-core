package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.Checker;

import java.nio.ByteBuffer;
import java.util.LinkedList;

// MagicNumber(short) + PayloadLength(Int) + PayloadChecksum(Long) + PayloadBytes(byte[]) + Ending(byte: 127)
class FrameCodec {
    final static byte ENDING_BYTE_VAL = 127;

    private final static int LEN_HEAD = Short.BYTES + Integer.BYTES + Long.BYTES;
    private final static int LEN_ENDING = Byte.BYTES;
    private final static int LEN_CHECK_SUM_ADD_ENDING = Long.BYTES + LEN_ENDING;
    private final static int LEN_HEAD_ADD_ENDING = LEN_HEAD + LEN_ENDING;
    private final static short MAGIC_NUMBER_V1 = 10;

    private int maxFrameLength;


    FrameCodec(int maxFrameLength) {
        this.maxFrameLength = maxFrameLength;
    }


    void encodeWrite(ByteBuffer byteBuffer, byte[] payload) {
        long checksum;
        try (Checker checker = Checker.getChecker();) {
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

    void decode(FrameCircularBuffer frameCircularBuffer, LinkedList<FrameMessage> frameMessages) {
        while (frameCircularBuffer.getLength() >= FrameCodec.LEN_HEAD) {
            frameCircularBuffer.markNextReadPos();
            short magicNumber = frameCircularBuffer.getShort();
            int payloadSize = frameCircularBuffer.getInt();
            if (magicNumber == FrameCodec.MAGIC_NUMBER_V1) {
                if (frameCircularBuffer.getLength() < payloadSize + LEN_CHECK_SUM_ADD_ENDING) {
                    frameCircularBuffer.resetNextReadPos2Mark();
                    break;
                } else {
                    long checksum0 = frameCircularBuffer.getLong();
                    byte[] payload = frameCircularBuffer.getNextBytes(payloadSize);
                    long checksum;
                    try (Checker checker = Checker.getChecker()) {
                        checker.update(payload, 0, payload.length);
                        checker.update(frameCircularBuffer.getByte());
                        checksum = checker.getValue();
                    }
                    if (checksum != checksum0) {
                        throw new IllegalArgumentException("checksum error [checksum0：" + checksum + ", checksum:" + checksum + "]");
                    }
                    FrameMessage frameMessage = new FrameMessage(
                            frameLengthAddPayloadLen(payloadSize)
                            , payload
                    );
                    frameMessages.add(frameMessage);
                }
            } else {
                throw new IllegalArgumentException("Illegal MAGIC_NUMBER # " + magicNumber);
            }
        }
    }


    int frameLengthAddPayloadLen(int payloadLen) {
        int frameLen = payloadLen + LEN_HEAD_ADD_ENDING;
        if (frameLen > maxFrameLength) {
            throw new IllegalArgumentException("frameLen:" + frameLen + ", maxFrameLength:" + maxFrameLength);
        }
        return frameLen;
    }
}
