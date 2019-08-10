package io.github.rapid.queue.core.kit;

public final class CircularBuffer {
    private final byte[] buffer;
    private final int capacity;
    private final int maxIndex;
    private int nextReadPos = 0;
    private int nextWritePos = 0;
    private int readPosMark;
    private boolean flipped = false;

    public CircularBuffer(int capacity) {
        this.capacity = capacity;
        this.maxIndex = this.capacity - 1;
        this.buffer = new byte[capacity];
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public int remaining_OneWay() {
        if (nextReadPos < nextWritePos) {
            return capacity - nextWritePos;
        } else if (nextReadPos == nextWritePos) {
            if (flipped) {
                return 0;
            } else {
                return capacity - nextWritePos;
            }
        } else {
            return nextReadPos - nextWritePos;
        }
    }

    private int remaining_Ring() {
        if (nextReadPos < nextWritePos) {
            return capacity - (nextWritePos - nextReadPos);
        } else if (nextReadPos == nextWritePos) {
            if (flipped) {
                return 0;
            } else {
                return capacity;
            }
        } else {
            return nextReadPos - nextWritePos;
        }
    }

    public int getLength() {
        return capacity - remaining_Ring();
    }

    public int getNextWritePos() {
        return nextWritePos;
    }

    void markNextReadPos() {
        this.readPosMark = nextReadPos;
    }

    void resetNextReadPos2Mark() {
        this.nextReadPos = readPosMark;
    }


    public void incrementAndGetWritePos(int len) {
        int remainingRing = remaining_Ring();
        if (remainingRing < len) {
            throw new IllegalArgumentException("incrementAndGetWritePos overflow");
        }

        int next = this.nextWritePos + len;
        if (next > this.maxIndex) {
            next = next - this.capacity;
            if (!flipped) {
                flipped = true;
            }
        }
        this.nextWritePos = next;
    }

    short getShort() {
        int length = getLength();
        if (length >= Short.BYTES) {
            return BytesKit.bytes2short(
                    _getNextByte()
                    , _getNextByte()
            );
        } else {
            throw new IllegalArgumentException("length is " + length);
        }
    }

    int getInt() {
        int length = getLength();
        if (length >= Integer.BYTES) {
            return BytesKit.bytes2int(
                    _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
            );
        } else {
            throw new IllegalArgumentException("length is " + length);
        }
    }

    long getLong() {
        int length = getLength();
        if (length >= Long.BYTES) {
            return BytesKit.bytes2long(
                    _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
                    , _getNextByte()
            );
        } else {
            throw new IllegalArgumentException("length is " + length);
        }
    }

    private byte _getNextByte() {
        byte b = buffer[nextReadPos];
        int next = this.nextReadPos + 1;
        if (next > this.maxIndex) {
            next = next - this.capacity;
            if (flipped) {
                flipped = false;
            }
        }
        this.nextReadPos = next;
        return b;
    }

    byte getByte() {
        int length = getLength();
        if (length >= Byte.BYTES) {
            return _getNextByte();
        } else {
            throw new IllegalArgumentException("length is " + length);
        }
    }

    byte[] getNextBytes(int readLen) {
        if (readLen == 0) {
            return new byte[0];
        }
        int length = getLength();
        if (length >= readLen) {
            byte[] rst = new byte[readLen];
            if (this.nextWritePos > this.nextReadPos) {
                System.arraycopy(buffer, nextReadPos, rst, 0, readLen);
                nextReadPos = nextReadPos + readLen;
                return rst;
            } else if (this.nextWritePos < this.nextReadPos || flipped) {
                int oneWayLen = capacity - this.nextReadPos;
                if (oneWayLen >= readLen) {
                    System.arraycopy(buffer, nextReadPos, rst, 0, readLen);
                    int next = nextReadPos + readLen;
                    if (next > maxIndex) {
                        this.nextReadPos = next - capacity;
                        if (flipped) {
                            flipped = false;
                        }
                    } else {
                        this.nextReadPos = next;
                    }
                } else {
                    System.arraycopy(buffer, nextReadPos, rst, 0, oneWayLen);
                    int remaining = readLen - oneWayLen;
                    System.arraycopy(buffer, 0, rst, oneWayLen, remaining);
                    this.nextReadPos = remaining;
                    if (flipped) {
                        flipped = false;
                    }
                }
                return rst;
            } else {
                throw new ImperfectException(-1, "getNextBytes got error");
            }
        } else {
            throw new IllegalArgumentException("length is " + length);
        }
    }


}
