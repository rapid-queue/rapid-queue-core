package io.github.rapid.queue.core.file;

import java.math.BigInteger;

abstract class StoreBase {

    final static String EXTENSION = "rq";
    final static String EXTENSION_DOT = "." + EXTENSION;
    final static int ZERO_PAGE_ID = 0;


    private static final long offsetPageHi = new BigInteger("10").pow(String.valueOf(Integer.MAX_VALUE).length()).longValue();

    static int[] offsetInfo(long offset) {
        long pageId = offset / offsetPageHi;
        long position = offset - pageId * offsetPageHi;
        return new int[]{Math.toIntExact(pageId), Math.toIntExact(position)};
    }


    static int compareOffset(long offsetId0, long offsetId1) {
        return Long.compare(offsetId0, offsetId1);
    }


    static long createOffset(int pageId, int bodyStartPosition) {
        return pageId * offsetPageHi + bodyStartPosition;
    }

}
