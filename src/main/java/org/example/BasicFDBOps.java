package org.example;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.*;

public class BasicFDBOps {

    private static Database db;
    public static void main(String[] args) {
        FDB fdb = FDB.selectAPIVersion(630);
        db = fdb.open();

        try {
            set("abc", "abc");
            get("abc");
            getRange("a", "n");
        } finally {
            db.close();
        }
    }

    // SET method to set the value of a key in the database
    public static void set(String key, String value) {
        db.run(tr -> {
            tr.set(key.getBytes(), value.getBytes());
            System.out.println("Set: " + key + " = " + value);
            return null;
        });
    }

    //GET method to retrieve the value of given key
    public static void get(String key) {
        db.run(tr -> {
            byte[] value = tr.get(key.getBytes()).join();
            System.out.println("Get: " + key + " = " + new String(value));
            return null;
        });
    }

    //GETRANGE method to retrieve values of a range of keys from the database
    public static void getRange(String startKey, String endKey) {
        db.run(tr -> {
            AsyncIterator<KeyValue> values = tr.getRange(startKey.getBytes(), endKey.getBytes()).iterator();

            System.out.println("\nGetRange: " + startKey + " to " + endKey);
            while (values.hasNext()) {
                KeyValue kv = values.next();
                System.out.println(new String(kv.getKey()) + " = " + new String(kv.getValue()));
            }
            return null;
        });
    }
}
