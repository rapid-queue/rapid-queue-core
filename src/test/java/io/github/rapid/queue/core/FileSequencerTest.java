package io.github.rapid.queue.core;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FileSequencerTest {
    private Sequencer fileSequencer;

    @Before
    public void setUp() throws Exception {
        File dataDir = new File("/data/seqTest1");
        FileUtils.deleteDirectory(dataDir);
        this.fileSequencer = Sequencer.createFileSequencerBuilder(dataDir).setCachePageSize(10).build();
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
    public void tryRepair1() throws IOException {
        int i = 0;
        long start = System.currentTimeMillis();
        try (SnapshotReader eventMessages = fileSequencer.readSnapshot(null)) {
            for (EventMessage eventMessage : eventMessages) {
                byte[] body = eventMessage.getBody();
                String string = new String(body);
                long offset = eventMessage.getOffset();
//                System.out.println(offset + "|"+string);
                if (Integer.parseInt(string) != i) {
                    throw new RuntimeException("error at :" + eventMessage);
                }
                i++;
            }
        }
        long l = System.currentTimeMillis() - start;
        System.out.println((i / l) * 1000);
        fileSequencer.close();
//        fileSequencer.append("a1", true);
//        fileSequencer.append("a2", true);
//        fileSequencer.append(JSON.toJSONString(Collections.singletonMap("a", "b")), true);
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
                    SequenceListener sequenceTailer
                            = fileSequencer.newMessageListener();
                    sequenceTailer.start(null, new MessageCallback() {
                        @Override
                        public void onMessage(EventMessage eventMessage) {
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

    @Test
    public void name2() throws IOException, InterruptedException {
        SequenceListener sequenceTailer
                = fileSequencer.newMessageListener();
        sequenceTailer.start(null, new MessageCallback() {
            @Override
            public void onMessage(EventMessage eventMessage) {
                System.out.println(eventMessage);
            }
        });
        while (true) {
            System.out.println(sequenceTailer.statusActive());
            TimeUnit.SECONDS.sleep(1);
        }
    }
}