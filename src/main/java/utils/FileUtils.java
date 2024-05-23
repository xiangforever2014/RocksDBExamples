package utils;

import java.io.File;

public class FileUtils {
	public static void deleteFolder(File folder) {
		if (folder.isDirectory()) {
			// 获取文件夹下的所有文件和子文件夹
			File[] files = folder.listFiles();
			if (files != null) {
				for (File file : files) {
					// 递归调用自身删除子文件夹
					deleteFolder(file);
				}
			}
		}

		// 删除文件夹
		folder.delete();
		// if (folder.delete()) {
		// 	System.out.println("Deleted: " + folder.getPath());
		// } else {
		// 	System.err.println("Failed to delete: " + folder.getPath());
		// }
	}
}
