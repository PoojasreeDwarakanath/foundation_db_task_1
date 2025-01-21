package org.example;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

class TransactionsSubtask8{

    FDB fdb = FDB.selectAPIVersion(630);

    public int T1() {
        try (Database db = fdb.open()) {
            db.run(tr -> {
                byte[] k1 = "abc".getBytes();
                byte[] v1 = tr.get(k1).join();
                System.out.println("T1: Getting value of abc - " + new String(v1));

                byte[] k2 = "a".getBytes();
                byte[] v2 = "a_t1".getBytes();
                tr.set(k2, v2);
                System.out.println("T1: Setting a to a_t1");
                return null;
            });
        }
        return 0;
    }

    public int T2(){
        try(Database db = fdb.open()) {
            db.run(tr -> {

                byte[] k1 = "a".getBytes();
                byte[] v1 = tr.get(k1).join();
                System.out.println("T2: Getting value of a - " + new String(v1));

                byte[] k2 = "abc".getBytes();
                byte[] v2 = "abc_t2".getBytes();
                tr.set(k2, v2);
                System.out.println("T2: Setting abc to abc_t2");

                // Uncomment the below code to help in debugging the value of abc after it is being set
//                byte[] k3 = "abc".getBytes();
//                byte[] v3 = tr.get(k3).join();
//                System.out.println("Getting value of abc - " + new String(v3));

                return null;
            });
        }
        return 0;
    }
}

public class TransactionConflict {
    public static void main(String[] args) {
        TransactionsSubtask8 transaction = new TransactionsSubtask8();

        Thread thread1 = new Thread(transaction::T1);
        Thread thread2 = new Thread(transaction::T2);

        thread1.start();
        thread2.start();

    }
}
