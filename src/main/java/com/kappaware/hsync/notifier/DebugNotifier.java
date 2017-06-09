package com.kappaware.hsync.notifier;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hsync.Utils;

public class DebugNotifier implements Notifier {
	private Path root;
	private String clientId;
	static Logger log = LoggerFactory.getLogger("Notifier");
	
	public DebugNotifier(Path root, String clientId) {
		this.root = root;
		this.clientId = clientId;
	}

	@Override
	public void fileRenamed(Path oldName, Path newName) {
		log.debug(String.format("clientId:%s, action:FILE_RENAMED, oldName:%s, newName:%s", this.clientId, Utils.concatPath(root, oldName), Utils.concatPath(root, newName)));
	}

	@Override
	public void folderCreated(Path path, String owner, String group, short mode) {
		log.debug(String.format("clientId:%s, action:FOLDER_CREATED, path:%s, owner:%s, group:%s, mode:%04d", this.clientId, Utils.concatPath(root, path), owner, group, mode));
	}

	@Override
	public void folderAdjusted(Path path, String owner, String group, short mode) {
		log.debug(String.format("clientId:%s, action:FOLDER_ADJUSTED, path:%s, owner:%s, group:%s, mode:%04d", this.clientId, Utils.concatPath(root, path), owner, group, mode));
	}

	@Override
	public void fileDeleted(Path path) {
		log.debug(String.format("clientId:%s, action:FILE°DELETED, path: %s", this.clientId, Utils.concatPath(root,  path)));
	}

	@Override
	public void startFileCopy(Path path) {
		log.debug(String.format("clientId:%s, action:COPY_STARTED, path:%s", this.clientId, Utils.concatPath(this.root, path)));
	}



	@Override
	public void fileCopied(Path path, String owner, String group, short mode, long size, long modTime) {
		log.debug(String.format("clientId:%s, action:FILE_COPIED, path:%s, owner:%s, group:%s, mode:%04d, size:%d, modificationTime:%s", this.clientId, Utils.concatPath(this.root, path), owner, group, mode, size, modTime));
	}

	
	
	@Override
	public void fileAdjusted(Path path, String owner, String group, short mode) {
		log.debug(String.format("clientId:%s, action:FILE_ADJUSTED, path:%s, owner:%s, group:%s, mode:%04d", this.clientId, Utils.concatPath(this.root, path), owner, group, mode));
	}

	@Override
	public void error(Path path, String message, Throwable t) {
		log.error(String.format("clientId: %s, action:ERROR, path:%s, message:%s, exception:%s", this.clientId, Utils.concatPath(this.root, path), message, t.toString()));
		//log.error(message, t);
	}


}
