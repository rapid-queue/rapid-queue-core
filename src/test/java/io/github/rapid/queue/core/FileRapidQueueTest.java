package io.github.rapid.queue.core;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FileRapidQueueTest {
    private RapidQueue fileSequencer;

    @Before
    public void setUp() throws Exception {
        File dataDir = new File("/data/seqTest1");
        FileUtils.deleteDirectory(dataDir);
        this.fileSequencer = RapidQueue.createFileSequencerBuilder(dataDir).setCachePageSize(10).build();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                fileSequencer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }


    @Test
    public void tryRepair() throws IOException {
        long start = System.currentTimeMillis();
        int MAX = 100000000;
        for (int i = 0; i < MAX; i++) {
            fileSequencer.append(String.valueOf(i).getBytes(), true);
        }
        long l = System.currentTimeMillis() - start;
        System.out.println((MAX / l) * 1000);
        fileSequencer.close();
        //2857000
    }


    @Test
    public void name() {
    }

    @Test
    public void newMessageListener() throws Exception {
        new Thread(() -> {
            for (int i = 0; i < 10000000; i++) {
                try {
                    long append = fileSequencer.append(String.valueOf(i).getBytes(), true);
                    if (append == -1) {
                        System.out.println(append);
                    }
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        HashMap<Integer, AtomicInteger> checkI = new HashMap<>();
        for (int i1 = 0; i1 < 100; i1++) {
            int finalI = i1;
            checkI.put(finalI, new AtomicInteger(-1));
            new Thread(() -> {
                try {
                    RapidQueueListener sequenceTailer
                            = fileSequencer.newMessageListener();
                    sequenceTailer.start(null, new RapidQueueCallback() {
                        @Override
                        public void onMessage(RapidQueueMessage eventMessage) {
                            String body = new String(eventMessage.getBody());
                            int message = Integer.parseInt(body);
                            AtomicInteger atomicInteger = checkI.get(finalI);
                            int last = message - 1;
                            if (last != atomicInteger.get()) {
                                System.out.println("ERROR。T:" + finalI + ", E:" + atomicInteger.get() + ", offset:" + eventMessage.getOffset() + " | C:" + body);
                            } else {
                            }
                            atomicInteger.set(message);
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            Thread.sleep(1000 * 5);
        }
        TimeUnit.MINUTES.sleep(5);
    }


}