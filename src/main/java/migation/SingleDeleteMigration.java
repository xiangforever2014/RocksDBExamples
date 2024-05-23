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

public class SingleDeleteMigration {
	public static void main(String[] args) throws RocksDBException {
		String dbPath = "./RocksdbDir/SingleDeleteMigration";
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

			System.out.println("Start to migrate data...");
			long startMigration = System.currentTimeMillis();
			try (
				RocksIterator it = db.newIterator(cf1);
				WriteBatch batch = new WriteBatch()) {
				// Iterate through the existing data in Column Family 1
				it.seekToFirst();
				while (it.isValid()) {
					// Put the key-value pairs to Column Family 2
					String migratedKey = "migrated_" + new String(it.key(), StandardCharsets.UTF_8);
					String migratedValue = "migrated_" + new String(it.value(), StandardCharsets.UTF_8);
					batch.put(cf1, migratedKey.getBytes(), migratedValue.getBytes());
					db.singleDelete(cf1, it.key());
					it.next();
				}
				// Write the key-value pairs to Column Family 1
				db.write(new WriteOptions(), batch);
				db.flush(new FlushOptions());
				db.compactRange(cf1);
			}
			long endMigration = System.currentTimeMillis();
			System.out.println("Migrate data finished, it cost " + (endMigration - startMigration) / 1000 + " seconds.");
			System.out.println("Start to check migration result...");
			System.out.println("Migration result -> " + DataUtils.checkMigationResult(db, cf1, DataUtils.DATA_NUM));
		}
	}
}
