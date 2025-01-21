package org.example;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

class TransactionsSubtask7 {

    FDB fdb = FDB.selectAPIVersion(630);

    public int T1() {
        try (Database db = fdb.open()) {
            db.run(tr -> {
                byte[] k1 = "abc".getBytes();
                byte[] v1 = tr.get(k1).join();
                System.out.println("T1: Getting value of abc - " + new String(v1));

                byte[] k2 = "a".getBytes();
                byte[] v2 = tr.get(k2).join();
                System.out.println("T1: Getting value of a - " + new String(v2));

                byte[] k3 = "aa".getBytes();
                byte[] v3 = tr.get(k3).join();
                System.out.println("T1: Getting value of aa - " + new String(v3));

                return null;
            });
        }
        return 0;
    }

    public int T2(){
        try(Database db = fdb.open()) {
            db.run(tr -> {
                byte[] k1 = "abc".getBytes();
                byte[] v1 = "abc_5".getBytes();
                tr.set(k1, v1);

                byte[] k2 = "abcde".getBytes();
                byte[] v2 = "abcde".getBytes();
                tr.set(k2, v2);

                return null;
            });
        }
        return 0;
    }
}

public class ReadSnapshot {
    public static void main(String[] args) {
        TransactionsSubtask7 transaction = new TransactionsSubtask7();

        Thread thread1 = new Thread(transaction::T1);
        Thread thread2 = new Thread(transaction::T2);

        thread1.start();
        thread2.start();

    }
}
