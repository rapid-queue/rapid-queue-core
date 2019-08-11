package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.ImperfectException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

final class StorePageWriter implements AutoCloseable, Closeable {
    private final static Logger logger = LoggerFactory.getLogger(StorePageWriter.class);
    //
    final int pageId;
    //
    private final StoreMessageHelper storeMessageHelper;
    private final int writerPerSize;
    private final FrameCodec frameCodec;
    //
    private AtomicInteger position = null;
    private RandomAccessFile randomAccessFile;
    private MappedByteBuffer writeBufferPage;

    static StorePageWriter createOpened(int pageId, StoreMessageHelper storeMessageHelper) throws IOException {
        StorePageWriter storePageWriter = new StorePageWriter(pageId, storeMessageHelper);
        storePageWriter.open();
        return storePageWriter;
    }

    private StorePageWriter(int pageId, StoreMessageHelper storeMessageHelper) {
        this.pageId = pageId;
        //
        this.storeMessageHelper = storeMessageHelper;
        this.frameCodec = this.storeMessageHelper.frameCodec;
        this.writerPerSize = storeMessageHelper.writerPerSize;
        //
    }

    private void open() throws IOException {
        File diskFile = storeMessageHelper.getDiskFile(pageId);
        FileUtils.touch(diskFile);
        this.randomAccessFile = new RandomAccessFile(diskFile, "rw");

        StorePageSummary summary = StorePageSummary.read(randomAccessFile);
        if (summary == null) {
            this.position = new AtomicInteger(StorePageSummary.SIZE);
        }
        else {
            if (summary.getFinalPageLength() != StorePageSummary.DEFAULT_FILE_INT
                    && summary.getPageLength() == StorePageSummary.DEFAULT_FILE_INT
            ) {
                throw new IllegalArgumentException("read only");
            }
            else if (summary.getFinalPageLength() == StorePageSummary.DEFAULT_FILE_INT
                    && summary.getPageLength() != StorePageSummary.DEFAULT_FILE_INT
            ) {
                this.position = new AtomicInteger(summary.getPageLength());
            }
            else if (summary.getFinalPageLength() == StorePageSummary.DEFAULT_FILE_INT
//                    && summary.getPageLength() == StorePageSummary.DEFAULT_FILE_INT
            ) {
                Integer lastEndingPos = null;
                int checkedPos = Math.toIntExact(this.randomAccessFile.length());
                int seekPos = checkedPos;
                byte[] bytes = new byte[Math.toIntExact(FileUtils.ONE_MB)];

                while (lastEndingPos == null) {
                    seekPos = Math.toIntExact(seekPos - bytes.length);
                    if (seekPos < StorePageSummary.SIZE) {
                        seekPos = StorePageSummary.SIZE;
                    }

                    this.randomAccessFile.seek(seekPos);
                    int read = this.randomAccessFile.read(bytes);
                    for (int i = read - 1; i >= 0; i--) {
                        checkedPos--;
                        byte b = bytes[i];
                        if (b != 0) {
                            if (b == FrameCodec.ENDING_BYTE_VAL) {
                                lastEndingPos = Math.toIntExact(checkedPos);
                                break;
                            } else {
                                throw new ImperfectException(pageId, "file is imperfect @ pos:" + checkedPos);
                            }
                        }
                    }
                    if (seekPos == StorePageSummary.SIZE && lastEndingPos == null) {
                        lastEndingPos = -1;
                        break;
                    }
                }
                int contentLength = lastEndingPos + 1;
                if (contentLength == 0) {
                    this.position = new AtomicInteger(StorePageSummary.SIZE);
                }
                else if (contentLength > 0) {
                    this.position = new AtomicInteger(StorePageSummary.SIZE);
                    try (StorePageReader storePageReader = StorePageReader.createOpened(pageId, storeMessageHelper, contentLength)) {
                        for (StorePageReaderFrame messageFrame : storePageReader.readFull(null)) {
                            this.position.addAndGet(messageFrame.frameMessage.getFrameLength());
                        }
                    }
                    logger.info("pageId:{} try repair position:{}, lastEndingPos:{}", pageId, position.get(), lastEndingPos);
                    if (this.position.get() != contentLength) {
                        throw new ImperfectException(pageId, "writePosition:" + this.getPosition() + " != lastEndingPos:" + lastEndingPos);
                    }
                }
            }
            else {
                throw new ImperfectException(pageId, "file is imperfect @ storePageSummary: " + summary);
            }
        }
        StorePageSummary.write(randomAccessFile, new StorePageSummary());
        this.writeBufferPage = this.randomAccessFile.getChannel()
                .map(FileChannel.MapMode.READ_WRITE
                        , position.get()
                        , this.writerPerSize);
    }

    int getPosition() {
        return position.get();
    }


    int append(byte[] body) throws IOException {
        int frameLength = frameCodec.frameLengthAddPayloadLen(body.length);
        if (frameLength > writeBufferPage.remaining()) {
            writeBufferPage.force();
            writeBufferPage = this.randomAccessFile.getChannel()
                    .map(FileChannel.MapMode.READ_WRITE
                            , position.get()
                            , writerPerSize);
        }
        frameCodec.encodeWrite(writeBufferPage, body);
        return position.getAndAdd(frameLength);
    }


    @Override
    public void close() throws IOException {
        writeBufferPage.force();
        StorePageSummary.write(randomAccessFile, new StorePageSummary().setPageLength(position.get()));
        randomAccessFile.close();
    }

    void finalAndClose() throws IOException {
        writeBufferPage.force();
        StorePageSummary.write(randomAccessFile, new StorePageSummary().setFinalPageLength(position.get()));
        randomAccessFile.close();
    }


}
