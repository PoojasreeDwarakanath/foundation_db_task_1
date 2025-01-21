package org.example;

import com.apple.foundationdb.*;
import java.util.List;
import java.util.concurrent.*;

public class SingleVsMultiRanges {
    private static final FDB fdb;
    private static final Database db;

    static {
        fdb = FDB.selectAPIVersion(630);
        db = fdb.open();
    }

    public static void main(String[] args) {

        ExecutorService executor = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(10);
        final int ROW_LIMIT_UNLIMITED = 0;

        for (int i = 0; i < 10; i++) {
            final int start = i * 1000;
            final int end = (i + 1) * 1000 + 1;
            CountDownLatch finalLatch = latch;
            executor.submit(() -> {
                try {
                    db.run((Transaction tr) -> {
                        byte[] key1 = new byte[]{(byte) start};
                        byte[] key2 = new byte[]{(byte) end};
                        return tr.getRange(key1, key2).asList().join();
                    });
                } finally {
                    finalLatch.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        latch = new CountDownLatch(10);

        long startTime = System.nanoTime();

        for (int i = 0; i < 10; i++) {
            final int start = i * 1000;
            final int end = (i + 1) * 1000 + 1;
            CountDownLatch finalLatch1 = latch;
            executor.submit(() -> {
                try {
                    db.run((Transaction tr) -> {
                        byte[] key1 = new byte[]{(byte) start};
                        byte[] key2 = new byte[]{(byte) end};
//                        Uncomment based on the required streaming mode
                        List<KeyValue> values = tr.getRange(key1, key2).asList().join();
//                        List<KeyValue> values = tr.getRange(key1, key2, ROW_LIMIT_UNLIMITED, false, StreamingMode.ITERATOR).asList().join();
//                        List<KeyValue> values = tr.getRange(key1, key2, 1000).asList().join();
                        return values;
                    });
                } finally {
                    finalLatch1.countDown();
                }
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        long executionTime = (endTime - startTime) / 1_000_000;

        System.out.println("Execution time for multiple getRanges in parallel: " + executionTime + " ms");

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

    }
}
