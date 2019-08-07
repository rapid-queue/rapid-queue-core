package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.EventMessage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

final class FileSequencerCircularCache {

    private volatile int nextWritePos = 0;
    private volatile boolean flipped = false;

    private final EventMessage[] buffer;
    private final int capacity;
    private final int maxIndex;

    FileSequencerCircularCache(int capacity) {
        this.buffer = new EventMessage[capacity];
        this.capacity = capacity;
        this.maxIndex = this.capacity - 1;
    }

    void add(EventMessage element) {
        buffer[nextWritePos] = element;
        int next = nextWritePos + 1;
        if (next > maxIndex) {
            next = next - capacity;
            if (!flipped) {
                flipped = true;
            }
        }
        this.nextWritePos = next;
    }

    private final FullReader fullReader = new FullReader();

    FullReader createReader(@Nullable Long offset) {
        //计算头尾
        EventMessage tailMsg;
        int head;
        int tail;
        if (nextWritePos == 0) {
            if (flipped) {
                //写的转圈了
                tailMsg = buffer[maxIndex];
                head = nextWritePos;
                tail = maxIndex;
            } else {
                tailMsg = null;
                head = nextWritePos;
                tail = nextWritePos;
            }
        } else if (nextWritePos > 0) {
            tailMsg = buffer[nextWritePos - 1];
            if (flipped) {
                //写的转圈了
                head = nextWritePos;
                tail = nextWritePos - 1;
            } else {
                head = 0;
                tail = nextWritePos - 1;
            }
        } else {
            throw new IllegalArgumentException("createReader error");
        }
        if (flipped && head == tail) {
            throw new IllegalArgumentException("head == tail && tailMsg != null");
        }
        if (tailMsg == null) {
            return fullReader
                    .setStatus(ReaderStatus.EMPTY)
                    .setHead(head)
                    .setTail(tail);
        } else {
            if (offset == null) {
                //这里是因为缓存里都有数据了，那么文件里肯定也有数据，那么就让去文件里查询
                return fullReader
                        .setStatus(ReaderStatus.LESS)
                        .setHead(head)
                        .setTail(tail);
            } else {
                if (StoreBase.compareOffset(offset, tailMsg.getOffset()) > 0) {
                    return fullReader
                            .setStatus(ReaderStatus.GREATER)
                            .setHead(head)
                            .setTail(tail);
                } else {
                    if (StoreBase.compareOffset(offset, buffer[head].getOffset()) < 0) {
                        return fullReader
                                .setStatus(ReaderStatus.LESS)
                                .setHead(head)
                                .setTail(tail);
                    } else {
                        return fullReader
                                .setStatus(ReaderStatus.WITHIN)
                                .setHead(head)
                                .setTail(tail);
                    }
                }
            }
        }
    }

    enum ReaderStatus {
        LESS, GREATER, WITHIN, EMPTY
    }

    public class FullReaderIterator implements Iterator<EventMessage> {
        private boolean circle;
        private int nextIx;

        private ReaderStatus status;
        private int head;
        private int tail;

        private Iterator<EventMessage> reset(ReaderStatus status, int head, int tail) {
            this.status = status;
            this.head = head;
            this.tail = tail;
            this.circle = head > tail;
            this.nextIx = -1;
            return this;
        }

        @Override
        public boolean hasNext() {
            if (status.equals(ReaderStatus.WITHIN)) {
                if (nextIx == -1) {
                    nextIx = head;
                    return true;
                } else {
                    int next = nextIx + 1;
                    if (circle) {
                        //如果需要转圈，那么先需要必有要到最大值
                        if (next > maxIndex) {
                            next = next - capacity;
                            circle = false;
                            this.nextIx = next;
                            return true;
                        } else {
                            this.nextIx = next;
                            return true;
                        }
                    } else {
                        //如果不需要转圈，那么判断是否到尾巴
                        if (next <= tail) {
                            this.nextIx = next;
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
            } else {
                return false;
            }
        }

        @Override
        public EventMessage next() {
            return buffer[nextIx];
        }
    }

    class FullReader implements Iterable<EventMessage> {
        private final FullReaderIterator fullReaderIterator = new FullReaderIterator();
        private ReaderStatus status;
        private int head;
        private int tail;

        FullReader setStatus(ReaderStatus status) {
            this.status = status;
            return this;
        }

        FullReader setHead(int head) {
            this.head = head;
            return this;
        }

        FullReader setTail(int tail) {
            this.tail = tail;
            return this;
        }

        ReaderStatus getStatus() {
            return status;
        }


        @Nonnull
        @Override
        public Iterator<EventMessage> iterator() {
            return fullReaderIterator.reset(status, head, tail);
        }
    }
}
