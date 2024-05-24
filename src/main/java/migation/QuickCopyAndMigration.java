package migation;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ExportImportFilesMetaData;
import org.rocksdb.FlushOptions;
import org.rocksdb.ImportColumnFamilyOptions;
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

public class QuickCopyAndMigration {
	public static void main(String[] args) throws RocksDBException {
		String dbPath = "./RocksdbDir/QuickCopyAndMigration";
		String exportDir = "./RocksdbDir/QuickCopyAndMigration/export";
		FileUtils.deleteFolder(new File(dbPath));
		FileUtils.deleteFolder(new File(exportDir));

		try (
			// Open the RocksDB database
			Options options = new Options().setCreateIfMissing(true);
			RocksDB db = RocksDB.open(options, dbPath);
			// Create a Column Family Handle for Column Family 1
			ColumnFamilyHandle cf1 = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()))
			) {
			// Write data to Column Family 1
			DataUtils.writeDataToDb(db, cf1);

			// Quick copy data from Column Family 1 to Column Family 2
			System.out.println("Start quick copy data from cf1 to cf2...");
			long startCopyTime = System.currentTimeMillis();
			Checkpoint checkpoint = Checkpoint.create(db);
			ExportImportFilesMetaData exportImportFilesMetaData = checkpoint.exportColumnFamily(cf1, exportDir);

			ColumnFamilyHandle columnFamilyWithImport = db.createColumnFamilyWithImport(
				new ColumnFamilyDescriptor("cf2".getBytes(), new ColumnFamilyOptions()),
				new ImportColumnFamilyOptions(),
				exportImportFilesMetaData);
			long endCopyTime = System.currentTimeMillis();
			System.out.println("Quick copy data from cf1 to cf2 time: " + (endCopyTime - startCopyTime) / 1000 + " seconds.");

			// Drop Column Family 1
			System.out.println("Start to drop cf1...");
			long startTime2 = System.currentTimeMillis();
			db.dropColumnFamily(cf1);
			long endTime2 = System.currentTimeMillis();
			System.out.println("drop cf1 time: " + (endTime2 - startTime2) + " ms.");

			// Migrate data from Column Family 2 to Column Family 1
			System.out.println("Start migrate data from cf2 to cf1...");
			long startTime = System.currentTimeMillis();
			try (RocksIterator it = db.newIterator(columnFamilyWithImport);
				 WriteBatch batch = new WriteBatch();
				 ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
			) {
				// Iterate through the existing data in Column Family 1
				it.seekToFirst();
				it.seekToLast();
				while (it.isValid()) {
					// Put the key-value pairs to Column Family 2
					String migratedKey = "migrated_" + new String(it.key(), StandardCharsets.UTF_8);
					String migratedValue = "migrated_" + new String(it.value(), StandardCharsets.UTF_8);
					batch.put(columnFamilyHandle, migratedKey.getBytes(), migratedValue.getBytes());
					it.next();
				}
				// Write the key-value pairs to Column Family 2
				db.write(new WriteOptions(), batch);
				db.flush(new FlushOptions());
				db.compactRange(columnFamilyHandle);
				long endTime = System.currentTimeMillis();
				System.out.println("Migrate data from cf1 to cf2 time: " + (endTime - startTime) / 1000 + " seconds.");

				// Drop Column Family 2
				System.out.println("Start to drop cf2...");
				long startTime3 = System.currentTimeMillis();
				db.dropColumnFamily(columnFamilyWithImport);
				long endTime3 = System.currentTimeMillis();
				System.out.println("drop cf2 time: " + (endTime3 - startTime3) + " ms.");

				// DataUtils.iterRocksdbDataAndPrint(db, columnFamily);
				System.out.println("Start to check migration result...");
				System.out.println("Migration result -> " + DataUtils.checkMigationResult(db, columnFamilyHandle, DataUtils.DATA_NUM));
			}
		}
	}
}
