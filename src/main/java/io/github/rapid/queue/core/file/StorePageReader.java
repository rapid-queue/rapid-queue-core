package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.*;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

final class StorePageReader implements AutoCloseable, Closeable {
    final int pageId;
    private final String readerId;
    //
    private final StoreMessageHelper storeMessageHelper;
    private Integer pageLength;
    //
    private RandomAccessFile randomAccessFile;

    static StorePageReader createOpened(int pageId, StoreMessageHelper storeMessageHelper, @Nullable Integer pageLength) throws IOException {
        StorePageReader storePageReader = new StorePageReader(pageId, storeMessageHelper, pageLength);
        storePageReader.open();
        storeMessageHelper.PageReaderCloseHook.put(storePageReader.readerId, storePageReader);
        return storePageReader;
    }

    private StorePageReader(int pageId, StoreMessageHelper storeMessageHelper, @Nullable Integer pageLength) {
        this.pageId = pageId;
        this.readerId = UUIDKit.randomUUID();
        this.pageLength = pageLength;
        //
        this.storeMessageHelper = storeMessageHelper;
        //
    }

    private void open() throws IOException {
        File diskFile = storeMessageHelper.getDiskFile(pageId);
        if (!diskFile.exists()) {
            throw new ImperfectException(pageId, "file not exists");
        }
        this.randomAccessFile = new RandomAccessFile(diskFile, "r");
        StorePageSummary summary = StorePageSummary.read(randomAccessFile);
        if (summary == null) {
            throw new ImperfectException(pageId, "file summary not exists");
        } else {
            if (summary.getFinalPageLength() != StorePageSummary.DEFAULT_FILE_INT && pageLength == null) {
                pageLength = summary.getFinalPageLength();
            }
        }
        if (pageLength == null) {
            throw new ImperfectException(pageId, "content length is null");
        }
    }

    private final static Iterable<StorePageReaderFrame> EMPTY_ITERABLE = () -> new Iterator<StorePageReaderFrame>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public StorePageReaderFrame next() {
            throw new NoSuchElementException();
        }
    };

    Iterable<StorePageReaderFrame> readFull(@Nullable Integer position) throws IOException {
        if (position == null) {
            position = StorePageSummary.SIZE;
        }
        int readLength = pageLength - position;
        if (readLength < 0) {
            throw new IllegalArgumentException("readLength (" + readLength + ") < 0 ");
        }
        if (readLength == 0) {
            return EMPTY_ITERABLE;
        }
        Integer finalPosition = position;
        randomAccessFile.seek(position);
        return () -> new Iterator<StorePageReaderFrame>() {
            private int pos = finalPosition;
            private LinkedList<FrameMessage> frameMessages = new LinkedList<>();
            private FrameCircularBuffer frameCircularBuffer = new FrameCircularBuffer(storeMessageHelper.readerPerSize);
            private int alreadyReadSize = 0;

            @Override
            public boolean hasNext() {
                try {
                    while (true) {
                        if (frameMessages.size() > 0) {
                            return true;
                        }
                        int len = frameCircularBuffer.remaining_OneWay();
                        if (alreadyReadSize + len > readLength) {
                            len = readLength - alreadyReadSize;
                        }
                        if (len == 0) {
                            if (frameCircularBuffer.getLength() > 0) {
                                throw new ImperfectException(pageId, "file is imperfect bufferRemind");
                            }
                            return false;
                        }
                        int read = randomAccessFile.read(frameCircularBuffer.getBuffer(), frameCircularBuffer.getNextWritePos(), len);

                        if (read > 0) {
                            frameCircularBuffer.incrementAndGetWritePos(read);
                            StorePageReader.this.storeMessageHelper.frameCodec.decode(frameCircularBuffer, frameMessages);
                            alreadyReadSize = alreadyReadSize + read;
                        } else {
                            if (frameCircularBuffer.getLength() > 0) {
                                throw new ImperfectException(pageId, "file is imperfect bufferRemind");
                            }
                            return false;
                        }
                    }
                } catch (Exception e) {
                    throw ImperfectException.pack(pageId, e.getMessage(), e);
                }
            }

            @Override
            public StorePageReaderFrame next() {
                FrameMessage frame = frameMessages.poll();
                if (frame != null) {
                    StorePageReaderFrame storePageReaderFrame
                            = new StorePageReaderFrame(pageId, pos, frame);
                    pos = pos + frame.getFrameLength();
                    return storePageReaderFrame;
                } else {
                    throw new ImperfectException(pageId, "messageFrames poll got null");
                }
            }
        };
    }


    @Override
    public void close() throws IOException {
        try {
            randomAccessFile.close();
        } finally {
            storeMessageHelper.PageReaderCloseHook.remove(readerId);
        }
    }
}
