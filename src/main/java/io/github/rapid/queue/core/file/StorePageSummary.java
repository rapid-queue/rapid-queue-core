package io.github.rapid.queue.core.file;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

@SuppressWarnings("WeakerAccess")
final public class StorePageSummary {
    final static int SIZE = Math.toIntExact((FileUtils.ONE_KB));

    final static int DEFAULT_FILE_INT = -1;

    private int finalPageLength = StorePageSummary.DEFAULT_FILE_INT;
    //这两个是临时关闭的时候有，运行的时候没有，如果有final的就没有这两个的
    private int pageLength = StorePageSummary.DEFAULT_FILE_INT;


    public int getFinalPageLength() {
        return finalPageLength;
    }

    public StorePageSummary setFinalPageLength(int finalPageLength) {
        this.finalPageLength = finalPageLength;
        return this;
    }

    public int getPageLength() {
        return pageLength;
    }

    public StorePageSummary setPageLength(int pageLength) {
        this.pageLength = pageLength;
        return this;
    }

    @Override
    public String toString() {
        return "StorePageSummary{" +
                "finalPageLength=" + finalPageLength +
                ", pageLength=" + pageLength +
                '}';
    }

    static StorePageSummary read(RandomAccessFile randomAccessFile) throws IOException {
        byte[] meta = new byte[SIZE];
        long filePointer = randomAccessFile.getFilePointer();
        randomAccessFile.seek(0);
        int read = randomAccessFile.read(meta);
        if (read == -1) {
            randomAccessFile.seek(filePointer);
            return null;
        } else {
            StorePageSummary storePageSummary = JSON.parseObject(new String(meta), StorePageSummary.class);
            randomAccessFile.seek(filePointer);
            return storePageSummary;
        }
    }

    private final static byte[] INIT_META = new byte[SIZE];

    static {
        Arrays.fill(INIT_META, (byte) 0);
    }

    static void write(RandomAccessFile randomAccessFile, StorePageSummary storePageSummary) throws IOException {
        long filePointer = randomAccessFile.getFilePointer();
        randomAccessFile.seek(0);
        randomAccessFile.write(INIT_META);
        randomAccessFile.seek(0);
        randomAccessFile.write(JSON.toJSONBytes(storePageSummary));
        randomAccessFile.seek(filePointer);
    }

}
