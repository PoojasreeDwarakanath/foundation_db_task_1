package org.example;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.*;
import com.apple.foundationdb.StreamingMode;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SingleGetRange {

    private FDB fdb = null;
    public SingleGetRange() {
        if (!FDB.isAPIVersionSelected()) {
            this.fdb = FDB.selectAPIVersion(630);
        }
    }

    public void setValues(){
        try(Database db = this.fdb.open()) {
            for (int i = 0; i <= 10000; i++) {
                String finalI = Integer.toString(i);
                db.run(tr -> {
                    byte[] key = finalI.getBytes();
                    byte[] value = finalI.getBytes();
                    tr.set(key, value);
                    return null;
                });
            }
        }

    }

    public void getRangeWantAll(Integer startRange, Integer endRange) {
        byte[] key1 = null;
        byte[] key2 = null;

        if (startRange == null && endRange == null) {
            key1 = new byte[] {(byte) 0x00};
            key2 = new byte[] {(byte) 0xff};
        }
        else{
            key1 = new byte[] {(byte) (int) startRange};
            key2 = new byte[] {(byte) (int) endRange};
        }


        try(Database db = this.fdb.open()) {

            byte[] finalKey1 = key1;
            byte[] finalKey2 = key2;
            db.run(tr -> {
                try {
                    List<KeyValue> values = tr.getRange(finalKey1, finalKey2).asList().get();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // Restore the interrupted status
                    System.out.println("Thread was interrupted: " + e.getMessage());
                } catch (ExecutionException e) {
                    System.out.println("Execution exception: " + e.getMessage());
                }

              // Uncomment the below lines of code to print the values retrieved:
//              AsyncIterator<KeyValue> values = tr.getRange(finalKey1, finalKey2).iterator();
//                System.out.println("\nGetRange - mode:WANTALL");
//                while (values.hasNext()) {
//                    KeyValue kv = values.next();
//                    System.out.println(new String(kv.getKey())+" "+new String(kv.getValue()));
//                }
                return null;
            });
        }
    }

    public void getRangeIterator() {
        final int ROW_LIMIT_UNLIMITED = 0;

        try(Database db = this.fdb.open()) {

            db.run(tr -> {
                byte[] key1 = new byte[] {(byte) 0x00}; //.getBytes();
                byte[] key2 = new byte[] {(byte) 0xff}; //.getBytes();
                try{
                List<KeyValue> values = tr.getRange(key1, key2, ROW_LIMIT_UNLIMITED, false, StreamingMode.ITERATOR).asList().get();
                // Trying out AsyncIterator<KeyValue> return type
                // AsyncIterator<KeyValue> values = tr.getRange(key1, key2, ROW_LIMIT_UNLIMITED, false, StreamingMode.ITERATOR).iterator();
              } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // Restore the interrupted status
                    System.out.println("Thread was interrupted: " + e.getMessage());
                } catch (ExecutionException e) {
                    System.out.println("Execution exception: " + e.getMessage());
                }
              return null;
            });
        }
    }

    public void getRangeExact() {

        try(Database db = this.fdb.open()) {

            db.run(tr -> {
                byte[] key1 = new byte[]{(byte) 0x00};
                byte[] key2 = new byte[]{(byte) 0xff};

                try {
                    List<KeyValue> values = tr.getRange(key1, key2, 50).asList().get();

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();  // Restore the interrupted status
                    System.out.println("Thread was interrupted: " + e.getMessage());
                } catch (ExecutionException e) {
                    System.out.println("Execution exception: " + e.getMessage());
                }
                return null;
            }
            );
        }
    }

    public static void main(String[] args) {

        long startTime = 0;
        long stopTime = 0;
        SingleGetRange singleGetRange = new SingleGetRange();

        // Uncomment the below line to store 10k key-value pairs
        // singleGetRange.setValues();

        // WANT_ALL mode
//        startTime = System.nanoTime();
//        singleGetRange.getRangeWantAll(null, null);
//        stopTime = System.nanoTime();
//        System.out.println("Time taken for default GetRange WANTALL mode: " + (stopTime - startTime)/1000000 + " ms");

        //ITERATOR mode
//        startTime = System.nanoTime();
//        singleGetRange.getRangeIterator();
//        stopTime = System.nanoTime();
//        System.out.println("Time taken for  GetRange ITERATOR mode: " + (stopTime - startTime)/1000000 + " ms");

        // EXACT mode
        startTime = System.nanoTime();
        singleGetRange.getRangeExact();
        stopTime = System.nanoTime();
        System.out.println("Time taken for  GetRange EXACT mode: " + (stopTime - startTime)/1000000 + " ms");

    }


}
