package io.github.rapid.queue.core.file;

import io.github.rapid.queue.core.kit.ImperfectException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;


final class StoreMessageHelper implements AutoCloseable {
    private final static Logger logger = LoggerFactory.getLogger(StoreMessageHelper.class);

    final ConcurrentHashMap<String, Closeable> PageReaderCloseHook = new ConcurrentHashMap<>();
    private final File dataDir;
    private final int maxPageSize;
    final int writerPerSize;
    final int readerPerSize;
    final FrameCodec frameCodec;

    private volatile StorePageWriter writer;

    File getDiskFile(int pageId) {
        return new File(dataDir, String.valueOf(pageId).concat(StoreBase.EXTENSION_DOT));
    }

    private List<Integer> dumpDiskFile() {
        Iterator<File> dirAllFiles = FileUtils.iterateFiles(dataDir
                , new String[]{StoreBase.EXTENSION}, false);
        List<Integer> rst = new ArrayList<>();
        while (dirAllFiles.hasNext()) {
            rst.add(Integer.valueOf(
                    FilenameUtils.getBaseName(
                            dirAllFiles.next().getPath()
                    ))
            );
        }
        return rst;
    }

    private void open() throws IOException {
        FileUtils.forceMkdir(dataDir);

        List<Integer> files = dumpDiskFile();
        if (files.size() == 0) {
            this.writer = StorePageWriter.createOpened(StoreBase.ZERO_PAGE_ID, this);
        } else {
            for (int i = 0; i < files.size(); i++) {
                if (!files.contains(i)) {
                    throw new ImperfectException(i, "page not imperfect page#" + i);
                }
            }
            this.writer = StorePageWriter.createOpened(files.size() - 1, this);
        }
    }

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void close() {
        try {
            writer.close();
        } catch (Exception e) {
            logger.warn(String.format("close writer error:%s, {}", writer), e);
        }
        for (Closeable value : PageReaderCloseHook.values()) {
            try {
                value.close();
            } catch (Exception e) {
                logger.warn(String.format("close error:%s, {}", value), e);
            }
        }
    }

    static StoreMessageHelper createOpened(File dataDir
            , int maxFrameLength, int maxPageSize
            , int writerPerSize, int readerPerSize) throws IOException {
        StoreMessageHelper storeMessageHelper = new StoreMessageHelper(dataDir, maxFrameLength, maxPageSize, writerPerSize, readerPerSize);
        storeMessageHelper.open();
        return storeMessageHelper;
    }

    private StoreMessageHelper(File dataDir
            , int maxFrameLength, int maxPageSize
            , int writerPerSize, int readerPerSize
    ) {
        this.dataDir = dataDir;
        this.maxPageSize = maxPageSize - maxFrameLength;
        this.writerPerSize = writerPerSize;
        this.readerPerSize = readerPerSize;
        if (maxPageSize < 0) {
            throw new IllegalArgumentException("maxPageSize(" + maxPageSize + ") <= maxFrameLength(" + maxFrameLength + ")");
        }
        if (writerPerSize > maxPageSize) {
            throw new IllegalArgumentException("writeBufferPageSize(" + writerPerSize + ") <= maxPageSize(" + maxPageSize + ")");
        }
        //
        this.frameCodec = new FrameCodec(maxFrameLength);
    }


    long writeAppend(byte[] body) throws IOException {
        if (closed.get()) {
            throw new IOException("store is closed");
        }
        if (body.length + writer.getPosition() > maxPageSize) {
            StorePageWriter finalWriter = this.writer;
            finalWriter.finalAndClose();
            this.writer = StorePageWriter.createOpened(finalWriter.pageId + 1, this);
        }

        int pos = writer.append(body);
        return StoreBase.createOffset(writer.pageId, pos);
    }


    StoreMessageReader readSnapshot(@Nullable Long offset) throws IOException {
        final StorePageWriter writer = this.writer;
        int writerPageId = writer.pageId;
        int writerPosition = writer.getPosition();
        if (offset == null) {
            return new StoreMessageReader(StoreBase.ZERO_PAGE_ID, null, writerPageId, writerPosition, this);
        } else {
            if (StoreBase.compareOffset(offset, StoreBase.createOffset(writerPageId, writerPosition)) > 0) {
                throw new IllegalArgumentException("offset is Greater than writer");
            }
            int[] offsetInfo = StoreBase.offsetInfo(offset);
            return new StoreMessageReader(offsetInfo[0], offsetInfo[1], writerPageId, writerPosition, this);
        }
    }
}

