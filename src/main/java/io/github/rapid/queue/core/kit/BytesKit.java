package io.github.rapid.queue.core.kit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public abstract class BytesKit {
    public static List<Byte> toList(byte[] array) {
        if (array == null) {
            return null;
        }

        List<Byte> list = new ArrayList<>(array.length);

        for (byte value : array) {
            list.add(value);
        }

        return list;
    }

    public static byte[] toArray(List<Byte> list) {
        if (list == null) {
            return null;
        }

        byte[] array = new byte[list.size()];

        int index = 0;
        for (byte value : list) {
            array[index++] = value;
        }

        return array;
    }


    public static void long2bytes(long value, byte[] bytes, int off) {
        bytes[off + 7] = (byte) value;
        bytes[off + 6] = (byte) (value >>> 8);
        bytes[off + 5] = (byte) (value >>> 16);
        bytes[off + 4] = (byte) (value >>> 24);
        bytes[off + 3] = (byte) (value >>> 32);
        bytes[off + 2] = (byte) (value >>> 40);
        bytes[off + 1] = (byte) (value >>> 48);
        bytes[off] = (byte) (value >>> 56);
    }

    static long bytes2long(byte[] bytes, int off) {
        return ((bytes[off + 7] & 0xFFL)) + ((bytes[off + 6] & 0xFFL) << 8) + ((bytes[off + 5] & 0xFFL) << 16)
                + ((bytes[off + 4] & 0xFFL) << 24) + ((bytes[off + 3] & 0xFFL) << 32) + ((bytes[off + 2] & 0xFFL) << 40)
                + ((bytes[off + 1] & 0xFFL) << 48) + (((long) bytes[off]) << 56);
    }

    public static long bytes2long(byte b0, byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7) {
        return ((b7 & 0xFFL)) + ((b6 & 0xFFL) << 8) + ((b5 & 0xFFL) << 16)
                + ((b4 & 0xFFL) << 24) + ((b3 & 0xFFL) << 32) + ((b2 & 0xFFL) << 40)
                + ((b1 & 0xFFL) << 48) + (((long) b0) << 56);
    }


    static void int2bytes(int value, byte[] bytes, int off) {
        bytes[off + 3] = (byte) value;
        bytes[off + 2] = (byte) (value >>> 8);
        bytes[off + 1] = (byte) (value >>> 16);
        bytes[off] = (byte) (value >>> 24);
    }


    static int bytes2int(byte[] bytes, int off) {
        return ((bytes[off + 3] & 0xFF)) + ((bytes[off + 2] & 0xFF) << 8) + ((bytes[off + 1] & 0xFF) << 16) + ((bytes[off]) << 24);
    }

    public static int bytes2int(byte b0, byte b1, byte b2, byte b3) {
        return ((b3 & 0xFF)) + ((b2 & 0xFF) << 8) + ((b1 & 0xFF) << 16) + (b0 << 24);
    }


    public static void short2bytes(short value, byte[] bytes, int off) {
        bytes[off + 1] = (byte) value;
        bytes[off] = (byte) (value >>> 8);
    }


    public static short bytes2short(byte[] b, int off) {
        return (short) (((b[off + 1] & 0xFF)) + ((b[off] & 0xFF) << 8));
    }

    public static short bytes2short(byte b0, byte b1) {
        return (short) (((b1 & 0xFF)) + ((b0 & 0xFF) << 8));
    }

    public static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        try (GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data);
            gzip.finish();
            return bos.toByteArray();
        }

    }

    public static byte[] unGzip(byte[] data) throws IOException {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(data))) {
            byte[] buf = new byte[2048];
            int size = -1;
            ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length + 1024);
            while ((size = gzip.read(buf, 0, buf.length)) != -1) {
                bos.write(buf, 0, size);
            }
            return bos.toByteArray();
        }

    }
}
