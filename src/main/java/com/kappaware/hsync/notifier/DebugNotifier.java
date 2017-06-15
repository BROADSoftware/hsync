package com.kappaware.hsync.notifier;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DebugNotifier extends NotifierFormater implements Notifier {
	static Logger log = LoggerFactory.getLogger(DebugNotifier.class);
	
	public DebugNotifier(Path root, String clientId) {
		super(root, clientId);
	}

	@Override
	public void fileRenamed(Path oldName, Path newName) {
		log.debug(this._fileRenamed(oldName, newName));
	}

	@Override
	public void folderCreated(Path path, String owner, String group, short mode) {
		log.debug(this._folderCreated(path, owner, group, mode));
	}

	@Override
	public void folderAdjusted(Path path, String owner, String group, short mode) {
		log.debug(this._folderAdjusted(path, owner, group, mode));
	}

	@Override
	public void fileDeleted(Path path) {
		log.debug(this._fileDeleted(path));
	}

	@Override
	public void startFileCopy(Path path) {
		log.debug(this._startFileCopy(path));
	}

	@Override
	public void fileCopied(Path path, String owner, String group, short mode, long size, long modTime) {
		log.debug(this._fileCopied(path, owner, group, mode, size, modTime));
	}
	
	@Override
	public void fileAdjusted(Path path, String owner, String group, short mode) {
		log.debug(this._fileAdjusted(path, owner, group, mode));
	}

	@Override
	public void error(Path path, String message, Throwable t) {
		log.debug(this._error(path, message, t));
		//log.error(message, t);
	}

	@Override
	public void close() {
	}


}
