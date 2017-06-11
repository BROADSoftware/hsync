package com.kappaware.hsync.notifier;

import org.apache.hadoop.fs.Path;

import com.kappaware.hsync.Utils;

public class NotifierFormater {
	private Path root;
	private String clientId;
	
	public NotifierFormater(Path root, String clientId) {
		this.root = root;
		this.clientId = clientId;
	}

	public String _fileRenamed(Path oldName, Path newName) {
		return String.format("clientId:%s, action:FILE_RENAMED, oldName:%s, newName:%s", this.clientId, Utils.concatPath(root, oldName), Utils.concatPath(root, newName));
	}

	public String _folderCreated(Path path, String owner, String group, short mode) {
		return String.format("clientId:%s, action:FOLDER_CREATED, path:%s, owner:%s, group:%s, mode:%04o", this.clientId, Utils.concatPath(root, path), owner, group, mode);
	}

	public String _folderAdjusted(Path path, String owner, String group, short mode) {
		return String.format("clientId:%s, action:FOLDER_ADJUSTED, path:%s, owner:%s, group:%s, mode:%04o", this.clientId, Utils.concatPath(root, path), owner, group, mode);
	}

	public String _fileDeleted(Path path) {
		return String.format("clientId:%s, action:FILE_DELETED, path: %s", this.clientId, Utils.concatPath(root,  path));
	}

	public String _startFileCopy(Path path) {
		return String.format("clientId:%s, action:COPY_STARTED, path:%s", this.clientId, Utils.concatPath(this.root, path));
	}

	public String _fileCopied(Path path, String owner, String group, short mode, long size, long modTime) {
		return String.format("clientId:%s, action:FILE_COPIED, path:%s, owner:%s, group:%s, mode:%04o, size:%d, modificationTime:%s", this.clientId, Utils.concatPath(this.root, path), owner, group, mode, size, modTime);
	}

	public String _fileAdjusted(Path path, String owner, String group, short mode) {
		return String.format("clientId:%s, action:FILE_ADJUSTED, path:%s, owner:%s, group:%s, mode:%04o", this.clientId, Utils.concatPath(this.root, path), owner, group, mode);
	}

	public String _error(Path path, String message, Throwable t) {
		return String.format("clientId: %s, action:ERROR, path:%s, message:%s, exception:%s", this.clientId, Utils.concatPath(this.root, path), message, t.toString());
		//log.error(message, t);
	}


}
