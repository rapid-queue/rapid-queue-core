package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.FrameCodec;
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
        //这里如果是写的那么就是文件的，那么就拿到文件
        File diskFile = storeMessageHelper.getDiskFile(pageId);
        FileUtils.touch(diskFile);
        this.randomAccessFile = new RandomAccessFile(diskFile, "rw");

        //初始化
        //读取 头文件
        StorePageSummary summary = StorePageSummary.read(randomAccessFile);
        if (summary == null) {
            //这个文件是一个新建的文件，写入头信息
            this.position = new AtomicInteger(StorePageSummary.SIZE);
        }
        //这里有读到信息，那么一定不是一个新进的文件
        else {
            //这里说明是正常关闭的，并且标记final的文件
            if (summary.getFinalPageLength() != StorePageSummary.DEFAULT_FILE_INT
                    && summary.getPageLength() == StorePageSummary.DEFAULT_FILE_INT
            ) {
                throw new IllegalArgumentException("read only");
            }
            //这里说明是正常关闭的，所以直接使用就可以了
            else if (summary.getFinalPageLength() == StorePageSummary.DEFAULT_FILE_INT
                    && summary.getPageLength() != StorePageSummary.DEFAULT_FILE_INT
            ) {
                this.position = new AtomicInteger(summary.getPageLength());
            }
            //这里说明是非正常关闭的，尝试是否可以自动修复
            else if (summary.getFinalPageLength() == StorePageSummary.DEFAULT_FILE_INT
//                    && summary.getPageLength() == StorePageSummary.DEFAULT_FILE_INT
            ) {
                //如果是可写的但是没有关闭时候的数据，那么需要读取整个文件，读取的同时也校验的每个帧的完整性，然后再恢复头数据
                //先找到最后一个写入点的Ending，倒着读文件
                Integer lastEndingPos = null;
                int checkedPos = Math.toIntExact(this.randomAccessFile.length());
                int seekPos = checkedPos;
                byte[] bytes = new byte[Math.toIntExact(FileUtils.ONE_MB)];

                //长度在字节位置的右边，位置在字节位的左边（坐标）
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
                                //这里确实找到了最后一个写入的位置
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
                //如果把文件都查询了一遍，但是还是没有找到pos, 那么就是这个里面本身就没有写入任何数据
                if (contentLength == 0) {
                    this.position = new AtomicInteger(StorePageSummary.SIZE);
                }
                //如果是找到了ending的话那么就假设这个位置是对的，那么就从头开始读，来进行验证
                else if (contentLength > 0) {
                    this.position = new AtomicInteger(StorePageSummary.SIZE);
                    try (StorePageReader storePageReader = StorePageReader.createOpened(pageId, storeMessageHelper, contentLength)) {
                        for (StorePageReaderFrame messageFrame : storePageReader.readFull(null)) {
                            this.position.addAndGet(messageFrame.messageFrame.getFrameLength());
                        }
                    }
                    logger.info("pageId:{} try repair position:{}, lastEndingPos:{}", pageId, position.get(), lastEndingPos);
                    //如果都读的是正确的那么就可以继续了,这里验证一次，因为如果数据是正常的，就是没有正常关闭那么长度应该是可以核对的
                    if (this.position.get() != contentLength) {
                        throw new ImperfectException(pageId, "writePosition:" + this.getPosition() + " != lastEndingPos:" + lastEndingPos);
                    }
                }
            }
            //这里说明都不满足，那么就异常处理了
            else {
                throw new ImperfectException(pageId, "file is imperfect @ storePageSummary: " + summary);
            }
        }
        //启动写入页,,清空文件头
        StorePageSummary.write(randomAccessFile, new StorePageSummary());
        this.writeBufferPage = this.randomAccessFile.getChannel()
                .map(FileChannel.MapMode.READ_WRITE
                        , position.get()
                        , this.writerPerSize);
    }

    int getPosition() {
        return position.get();
    }


    //返回写入的开始位置
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
