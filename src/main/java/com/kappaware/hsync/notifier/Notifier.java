package com.kappaware.hsync.notifier;

import org.apache.hadoop.fs.Path;

/**
 * All path are relative to targetRoot
 * @author sa
 *
 */
public interface Notifier {
	
	void fileRenamed(Path oldName, Path newName);

	void folderCreated(Path path, String owner, String group, short mode);

	void folderAdjusted(Path path, String owner, String group, short mode);

	void fileDeleted(Path path);
	
	void startFileCopy(Path path);

	void fileCopied(Path path, String owner, String group, short mode, long size, long modTime);

	void fileAdjusted(Path path, String owner, String group, short mode);
	
	void error(Path path, String message, Throwable t);

	void close();
}
