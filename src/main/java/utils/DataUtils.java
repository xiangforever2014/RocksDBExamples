package utils;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;

public class DataUtils {
	public static final long DATA_NUM = 2000000;
//	public static final long DATA_NUM = 1024 * 1024 * 1024; // 1GB data size

	public static boolean checkMigationResult(RocksDB db, ColumnFamilyHandle cf, long dataSize) {
		String keyPrefix = "migrated_key_";
		String valuePrefix = "migrated_value_";
		int digits = (int)(Math.log10(dataSize) + 1);
		Function<Integer, String> checkKeyFunc = id -> keyPrefix + String.format("%0" + digits + "d", id);
		Function<Integer, String> checkValueFunc = id -> valuePrefix + String.format("%0" + digits + "d", id);

		System.out.println("Start to check data...");
		long start = System.currentTimeMillis();
		try (RocksIterator it = db.newIterator(cf)) {
			it.seekToFirst();
			int index = 0;
			while (it.isValid()) {
				byte[] key = it.key();
				byte[] value = it.value();
				String keyStr = new String(key, StandardCharsets.UTF_8);
				String valueStr = new String(value, StandardCharsets.UTF_8);
				if (!keyStr.equals(checkKeyFunc.apply(index)) || !valueStr.equals(checkValueFunc.apply(index))) {
					System.out.println("Key or Value is not match with expected value");
					System.out.println("Expected Key: " + checkKeyFunc.apply(index));
					System.out.println("Actual Key: " + keyStr);
					System.out.println("Expected Value: " + checkValueFunc.apply(index));
					System.out.println("Actual Value: " + valueStr);
					return false;
				}
				index++;
				it.next();
			}
			if (index != dataSize) {
				System.out.println("Data number is not equal to dataSize");
				return false;
			}
			long end = System.currentTimeMillis();
			System.out.println("Check data finished, it cost " + (end - start) / 1000 + " seconds.");

			return true;
		}
	}

	public static boolean checkOldDataHasBeenRemoved(RocksDB db, ColumnFamilyHandle cf, long dataSize) {
		String keyPrefix = "key_";
		int digits = (int)(Math.log10(dataSize) + 1);
		Function<Integer, String> checkKeyFunc = id -> keyPrefix + String.format("%0" + digits + "d", id);

		System.out.println("Start to check data...");
		long start = System.currentTimeMillis();
		try (RocksIterator it = db.newIterator(cf)) {
			it.seekToFirst();
			int index = 0;
			while (it.isValid()) {
				byte[] value = db.get(checkKeyFunc.apply(index).getBytes());
				if (value != null) {
					System.out.println("Key [" + checkKeyFunc.apply(index) + "] is not removed");
					System.out.println("Value: " + new String(value, StandardCharsets.UTF_8));
					return false;

				}
				index++;
				it.next();
			}
			if (index != dataSize) {
				System.out.println("Data number is not equal to dataSize");
				return false;
			}
			long end = System.currentTimeMillis();
			System.out.println("Check data finished, it cost " + (end - start) / 1000.0 + " ms.");

			return true;
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	public static void iterRocksdbDataAndPrint(RocksDB db, ColumnFamilyHandle cf) {
		try (RocksIterator iter = db.newIterator(cf)) {
			// Iterate through the existing data in Column Family 1
			int datanum = 0;
			iter.seekToFirst();
			while (iter.isValid()) {
				byte[] key = iter.key();
				byte[] value = iter.value();
				System.out.println("Key: " + new String(key, StandardCharsets.UTF_8) + ", Value: " + new String(value, StandardCharsets.UTF_8));
				datanum++;
				iter.next();
			}
			System.out.println(datanum);
		}
	}

	public static void writeDataToDb(RocksDB db, ColumnFamilyHandle cf) throws RocksDBException {
		DataGenerator dataGenerator = new DataGenerator(DataUtils.DATA_NUM);
		System.out.println("Write " + DataUtils.DATA_NUM + " records to db...");
		long start = System.currentTimeMillis();
		for (String iterValue : dataGenerator) {
			String key = "key_" + iterValue;
			String value = "value_" + iterValue;
			db.put(cf, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
		}
		long end = System.currentTimeMillis();
		System.out.println("Write data to db finished, it cost " + (end - start) / 1000 + " seconds.");
	}

	public static long getRecordNumberOfCf(RocksDB db, ColumnFamilyHandle cf) throws RocksDBException {
		long start = System.currentTimeMillis();
		long dataNumInColumnFamily = 0;
		try (
			RocksIterator iter = db.newIterator(cf)) {
			iter.seekToFirst();
			while (iter.isValid()) {
				dataNumInColumnFamily++;
				iter.next();
			}
		}
		System.out.println("Data number in " + new String(cf.getName(), StandardCharsets.UTF_8) + " is [" + dataNumInColumnFamily + "]");
		long end = System.currentTimeMillis();
		System.out.println("Get record number of cf finished, it cost " + (end - start) + " milliseconds.");
		return dataNumInColumnFamily;
	}
}
