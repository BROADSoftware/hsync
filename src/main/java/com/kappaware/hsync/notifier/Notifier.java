package com.kappaware.hsync.notifier;


/**
 * All path are relative to targetRoot
 * @author sa
 *
 */
public interface Notifier {
	
	void fileRenamed(String oldName, String newName);

	void folderCreated(String path, String owner, String group, short mode);

	void folderAdjusted(String path, String owner, String group, short mode);

	void fileDeleted(String path);
	
	void copyStarted(String path);

	void fileCopied(String path, String owner, String group, short mode, long size, long modTime);

	void fileAdjusted(String path, String owner, String group, short mode);
	
	void error(String path, String message, Throwable t);

	void close();
}
