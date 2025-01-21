package org.example;

import com.apple.foundationdb.FDB;

class SingletonConnection{
    private FDB fdb = FDB.selectAPIVersion(630);
}

public class FDBConnection {
    SingletonConnection fdbConnection = new SingletonConnection();
}
