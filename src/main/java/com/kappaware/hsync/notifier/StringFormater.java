package com.kappaware.hsync.notifier;


public class StringFormater {
	private String clientId;
	
	public StringFormater(String clientId) {
		this.clientId = clientId;
	}

	public String _fileRenamed(String oldName, String newName) {
		return String.format("clientId:%s, action:FILE_RENAMED, oldName:%s, newName:%s", this.clientId, oldName, newName);
	}

	public String _folderCreated(String path, String owner, String group, short mode) {
		return String.format("clientId:%s, action:FOLDER_CREATED, path:%s, owner:%s, group:%s, mode:%04o", this.clientId, path, owner, group, mode);
	}

	public String _folderAdjusted(String path, String owner, String group, short mode) {
		return String.format("clientId:%s, action:FOLDER_ADJUSTED, path:%s, owner:%s, group:%s, mode:%04o", this.clientId, path, owner, group, mode);
	}

	public String _fileDeleted(String path) {
		return String.format("clientId:%s, action:FILE_DELETED, path:%s", this.clientId, path);
	}

	public String _copyStarted(String path) {
		return String.format("clientId:%s, action:COPY_STARTED, path:%s", this.clientId, path);
	}

	public String _fileCopied(String path, String owner, String group, short mode, long size, long modTime) {
		return String.format("clientId:%s, action:FILE_COPIED, path:%s, owner:%s, group:%s, mode:%04o, size:%d, modificationTime:%d", this.clientId, path, owner, group, mode, size, modTime);
	}

	public String _fileAdjusted(String path, String owner, String group, short mode) {
		return String.format("clientId:%s, action:FILE_ADJUSTED, path:%s, owner:%s, group:%s, mode:%04o", this.clientId, path, owner, group, mode);
	}

	public String _error(String path, String message, Throwable t) {
		return String.format("clientId: %s, action:ERROR, path:%s, message:%s, exception:%s", this.clientId, path, message, t.toString());
		//log.error(message, t);
	}


}
