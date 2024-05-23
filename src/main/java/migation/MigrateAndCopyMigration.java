package migation;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import utils.DataUtils;
import utils.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class MigrateAndCopyMigration {
	public static void main(String[] args) throws RocksDBException {
		String dbPath = "./RocksdbDir/MigrateAndCopyMigration";
		FileUtils.deleteFolder(new File(dbPath));

		try (
			// Open the RocksDB database
			Options options = new Options().setCreateIfMissing(true);
			RocksDB db = RocksDB.open(options, dbPath);
			// Create a Column Family Handle for Column Family 1
			ColumnFamilyHandle cf1 = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
			// Create a Column Family Handle for Column Family 2
			ColumnFamilyHandle cf2 = db.createColumnFamily(new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()))
			) {
			// Write data to Column Family 1
			DataUtils.writeDataToDb(db, cf1);

			// Migrate data from Column Family 1 to Column Family 2
			System.out.println("Start migrate data from cf1 to cf2...");
			long startTime = System.currentTimeMillis();
			try (RocksIterator it = db.newIterator(cf1);
				 WriteBatch batch = new WriteBatch()) {
				// Iterate through the existing data in Column Family 1
				it.seekToFirst();
				while (it.isValid()) {
					// Put the key-value pairs to Column Family 2
					String migratedKey = "migrated_" + new String(it.key(), StandardCharsets.UTF_8);
					String migratedValue = "migrated_" + new String(it.value(), StandardCharsets.UTF_8);
					batch.put(cf2, migratedKey.getBytes(), migratedValue.getBytes());
					it.next();
				}
				// Write the key-value pairs to Column Family 2
				db.write(new WriteOptions(), batch);
				db.flush(new FlushOptions());
				db.compactRange(cf2);
			}
			long endTime = System.currentTimeMillis();
			System.out.println("Migrate data from cf1 to cf2 time: " + (endTime - startTime) / 1000 + " seconds.");

			// Drop Column Family 1
			System.out.println("Start to drop cf1...");
			long startTime2 = System.currentTimeMillis();
			db.dropColumnFamily(cf1);
			long endTime2 = System.currentTimeMillis();
			System.out.println("drop cf1 time: " + (endTime2 - startTime2) + " ms.");

			// Copy data from Column Family 2 to Column Family 1
			System.out.println("Start to copy data from cf2 to cf1...");
			long startTime3 = System.currentTimeMillis();
			try (
				RocksIterator it = db.newIterator(cf2);
				WriteBatch batch = new WriteBatch();
				// Create a Column Family Handle for Column Family 1
				ColumnFamilyHandle columnFamily = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()))) {
				// Iterate through the existing data in Column Family 1
				it.seekToFirst();
				while (it.isValid()) {
					batch.put(columnFamily, it.key(), it.value());
					it.next();
				}
				// Write the key-value pairs to Column Family 1
				db.write(new WriteOptions(), batch);
				db.flush(new FlushOptions());
				db.compactRange(columnFamily);

				// DataUtils.iterRocksdbDataAndPrint(db, columnFamily);

				System.out.println("Copy data finished...");
				long endTime3 = System.currentTimeMillis();
				System.out.println("Copy data from cf2 to cf1 time: " + (endTime3 - startTime3) / 1000 + " seconds.");

				System.out.println("Start to check migration result...");
				System.out.println("Migration result -> " + DataUtils.checkMigationResult(db, columnFamily, DataUtils.DATA_NUM));

			}
		}
	}
}
