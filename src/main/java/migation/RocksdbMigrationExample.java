package migation;

import org.rocksdb.*;

import java.nio.charset.StandardCharsets;

public class RocksdbMigrationExample {
    public static void main(String[] args) throws RocksDBException {
        // Open the RocksDB database
        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, "./RocksdbDir/MigrateCF");

        // Create a Column Family Handle for Column Family 1
        ColumnFamilyHandle cf1 = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));

        // Create a Column Family Handle for Column Family 2
        ColumnFamilyHandle cf2 = db.createColumnFamily(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()));

        System.out.println("Write data to cf1...");
        // Write 20M data to Column Family 1
        // int dataSize = 20 * 1024 * 1024; // 20M
        int dataSize = 10000;
        for (int i = 0; i < dataSize; i++) {
            String key = "key_" + i;
            String value = "value_" + i;
            db.put(cf1, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        }

        System.out.println("Write data to cf1 finished.");

        // Iterate through the existing data in Column Family 1
        RocksIterator it = db.newIterator(cf1);
        it.seekToFirst();
        WriteBatch batch = new WriteBatch();
        while (it.isValid()) {
            // Put the key-value pairs to Column Family 2
            String migratedKey = "migrated_" + new String(it.key(), StandardCharsets.UTF_8);
            String migratedValue = "migrated_" + new String(it.value(), StandardCharsets.UTF_8);
            batch.put(cf2, migratedKey.getBytes(),  migratedValue.getBytes());
            it.next();
        }

        System.out.println("Write data to cf2 finished.");
        // Write the key-value pairs to Column Family 2
        db.write(new WriteOptions(), batch);

        // Drop Column Family 1
        db.dropColumnFamily(cf1);
        db.flush(new FlushOptions());
        db.compactRange();

        // Iterate through the existing data in Column Family 2
        RocksIterator it2 = db.newIterator(cf2);
        it2.seekToFirst();
        while (it2.isValid()) {
            byte[] key = it2.key();
            byte[] value = it2.value();
            System.out.println("Key: " + new String(key, StandardCharsets.UTF_8) + ", Value: " + new String(value, StandardCharsets.UTF_8));
            it2.next();
        }

        byte[] valueBytes = db.get(cf1, "key_100".getBytes());
        byte[] valueBytes1 = db.get(cf1, "migrated_key_100".getBytes());
        System.out.println("cf1, key_100-->" + (valueBytes == null ? "null" : new String(valueBytes, StandardCharsets.UTF_8)));
        System.out.println("cf1, migrated_key_100-->" + (valueBytes1 == null ? "null" : new String(valueBytes1, StandardCharsets.UTF_8)));

        byte[] valueBytes2 = db.get(cf2, "key_100".getBytes());
        byte[] valueBytes3 = db.get(cf2, "migrated_key_100".getBytes());
        System.out.println("cf2, key_100-->" + (valueBytes2 == null ? "null" : new String(valueBytes2, StandardCharsets.UTF_8)));
        System.out.println("cf2, migrated_key_100-->" + (valueBytes3 == null ? "null" : new String(valueBytes3, StandardCharsets.UTF_8)));


        // Close the database
        db.close();
    }
}