package migation;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileWriter;
import utils.DataUtils;
import utils.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;

public class FileWriterAndIngestDbMigration {
	private static final String SST_FILE_PREFIX = "ingest_";
	private static final String SST_FILE_SUFFIX = ".sst";
	private static final String SST_FILE_WRITER_TMP_DIR = "./RocksdbDir/SstFileWriterTmp";

	public static void main(String[] args) throws RocksDBException {
		String dbPath = "./RocksdbDir/FileWriterAndIngestDbMigration";
		FileUtils.deleteFolder(new File(dbPath));
		FileUtils.deleteFolder(new File(SST_FILE_WRITER_TMP_DIR));

		try (
			// Open the RocksDB database
			Options options = new Options().setCreateIfMissing(true);
			RocksDB db = RocksDB.open(options, dbPath);
			// Create a Column Family Handle for Column Family 1
			ColumnFamilyHandle cf1 = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()));
		) {
			// Write data to Column Family 1
			DataUtils.writeDataToDb(db, cf1);

			System.out.println("Start to migrate data from cf1 to sst files...");
			long startSSTFileWriter = System.currentTimeMillis();
			String absolutePath;
			// Use sst file writer to migrate data from cf1
			File ingestDir = new File(SST_FILE_WRITER_TMP_DIR, UUID.randomUUID().toString());
			if (!ingestDir.mkdirs()) {
				System.out.println("Dir " + ingestDir.getAbsolutePath() + "does not exist and cannot be created.");
				return;
			}
			try (
				final SstFileWriter sstFileWriter = new SstFileWriter(new EnvOptions(), options);
				RocksIterator it = db.newIterator(cf1)
			) {
				Path sstFilePath = Paths.get(ingestDir.getAbsolutePath(), SST_FILE_PREFIX + "cf1" + SST_FILE_SUFFIX);
				absolutePath = sstFilePath.toFile().getAbsolutePath();
				sstFileWriter.open(absolutePath);

				it.seekToFirst();
				// Iterate through the existing data in Column Family 1
				while (it.isValid()) {
					// Put the key-value pairs to Column Family 2
					String migratedKey = "migrated_" + new String(it.key(), StandardCharsets.UTF_8);
					String migratedValue = "migrated_" + new String(it.value(), StandardCharsets.UTF_8);
					sstFileWriter.put(migratedKey.getBytes(), migratedValue.getBytes());
					it.next();
				}

				sstFileWriter.finish();
			}

			long endSSTFileWriter = System.currentTimeMillis();
			System.out.println("Migrate data from cf1 to sst files finished. Time elapsed: " + (endSSTFileWriter - startSSTFileWriter) / 1000 + " seconds.");

			System.out.println("Drop cf1...");
			long startDropCf1 = System.currentTimeMillis();
			db.dropColumnFamily(cf1);
			db.compactRange();
			long endDropCf1 = System.currentTimeMillis();
			System.out.println("Drop cf1 finished. Time elapsed: " + (endDropCf1 - startDropCf1) / 1000 + " seconds.");

			System.out.println("Start to ingest sst files to db...");
			long startIngest = System.currentTimeMillis();
			// Ingest the migrated sst file back into cf1
			try (ColumnFamilyHandle columnFamily = db.createColumnFamily(new ColumnFamilyDescriptor("cf1".getBytes(), new ColumnFamilyOptions()))) {
				db.ingestExternalFile(columnFamily, Collections.singletonList(absolutePath), new IngestExternalFileOptions());
				long endIngest = System.currentTimeMillis();
				System.out.println("Ingest sst file back into cf1 finished. Time elapsed: " + (endIngest - startIngest) / 1000 + " seconds.");

				DataUtils.checkMigationResult(db, columnFamily, DataUtils.DATA_NUM);
			} catch (RocksDBException e) {
				e.printStackTrace();
			}
		}
	}
}
