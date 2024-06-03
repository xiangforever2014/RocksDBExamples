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

public class IteratorAndMigration {
	public static void main(String[] args) throws RocksDBException {
		String dbPath = "./RocksdbDir/IteratorAndMigration";
		FileUtils.deleteFolder(new File(dbPath));
		try (
			// Open the RocksDB database
			Options options = new Options().setCreateIfMissing(true);
			RocksDB db = RocksDB.open(options, dbPath);
			// Create a Column Family Handle for Column Family 1
			ColumnFamilyHandle cf1 = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()))
			) {
			// Write data to Column Family 1
			DataUtils.writeDataToDb(db, cf1);

			ColumnFamilyHandle columnFamily = null;
			try (
				RocksIterator it = db.newIterator(cf1);
				WriteBatch batch = new WriteBatch()) {
				it.seekToFirst();

				byte[] bytes0 = db.get(cf1, "key_1000000".getBytes());
				if(bytes0 == null) {
					System.out.println("key_1000000 not found in cf1");
				} else{
					System.out.println("Get value of key_1000000: " + new String(bytes0, StandardCharsets.UTF_8));
				}

				System.out.println("cf1 = " + cf1);
				System.out.println("cf1.getNativeHandle() = " + cf1.getNativeHandle());
				DataUtils.getRecordNumberOfCf(db, cf1);
				System.out.println("Start to drop cf1...");
				long startDrop = System.currentTimeMillis();
				System.out.println("drop cf1");
				// Drop Column Family 1 看起来是删除了cf1，但是实际上只是把cf1的handle给删除了，数据还是存在的
				db.dropColumnFamily(cf1);
				db.compactRange();

				long endDrop = System.currentTimeMillis();
				System.out.println("Drop cf1 finished, time elapsed: " + (endDrop - startDrop) + " ms");

				columnFamily = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));

				System.out.println("new cf1 = " + columnFamily);
				System.out.println("new cf1.getNativeHandle() = " + columnFamily.getNativeHandle());

				DataUtils.getRecordNumberOfCf(db, columnFamily);

				byte[] bytes = db.get(columnFamily, "key_1000000".getBytes());
				if(bytes == null) {
					System.out.println("key_1000000 not found in cf1");
				} else{
					System.out.println("Get value of key_1000000: " + new String(bytes, StandardCharsets.UTF_8));
				}

				byte[] bytes1 = db.get(cf1, "key_1000000".getBytes());
				if(bytes == null) {
					System.out.println("key_1000000 not found in cf1");
				} else{
					System.out.println("Get value of key_1000000: " + new String(bytes1, StandardCharsets.UTF_8));
				}

				System.out.println("Start to migrate data...");
				long startMigration = System.currentTimeMillis();

				// Iterate through the existing data in Column Family 1
				while (it.isValid()) {
					// Put the key-value pairs to Column Family 2
					String migratedKey = "migrated_" + new String(it.key(), StandardCharsets.UTF_8);
					String migratedValue = "migrated_" + new String(it.value(), StandardCharsets.UTF_8);
					batch.put(columnFamily, migratedKey.getBytes(), migratedValue.getBytes());
					it.next();
				}
				// Write the key-value pairs to Column Family 1
				db.write(new WriteOptions(), batch);
				db.flush(new FlushOptions());
				db.compactRange(columnFamily);

				long endMigration = System.currentTimeMillis();
				System.out.println("Migrate data finished, it cost " + (endMigration - startMigration) / 1000 + " seconds.");
				DataUtils.getRecordNumberOfCf(db, columnFamily);
			}

			System.out.println("Start to check migration result...");
			System.out.println("Migration result -> " + DataUtils.checkMigationResult(db, columnFamily, DataUtils.DATA_NUM));
			columnFamily.close();

			// DataUtils.iterRocksdbDataAndPrint(db, columnFamily);
			// DataUtils.iterRocksdbDataAndPrint(db, cf1);
		}
	}
}
