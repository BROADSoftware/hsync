package com.kappaware.hsync.notifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DebugNotifier extends StringFormater implements Notifier {
	static Logger log = LoggerFactory.getLogger(DebugNotifier.class);
	
	public DebugNotifier(String clientId) {
		super(clientId);
	}

	@Override
	public void fileRenamed(String oldName, String newName) {
		log.debug(this._fileRenamed(oldName, newName));
	}

	@Override
	public void folderCreated(String path, String owner, String group, short mode) {
		log.debug(this._folderCreated(path, owner, group, mode));
	}

	@Override
	public void folderAdjusted(String path, String owner, String group, short mode) {
		log.debug(this._folderAdjusted(path, owner, group, mode));
	}

	@Override
	public void fileDeleted(String path) {
		log.debug(this._fileDeleted(path));
	}

	@Override
	public void copyStarted(String path) {
		log.debug(this._copyStarted(path));
	}

	@Override
	public void fileCopied(String path, String owner, String group, short mode, long size, long modTime) {
		log.debug(this._fileCopied(path, owner, group, mode, size, modTime));
	}
	
	@Override
	public void fileAdjusted(String path, String owner, String group, short mode) {
		log.debug(this._fileAdjusted(path, owner, group, mode));
	}

	@Override
	public void error(String path, String message, Throwable t) {
		log.debug(this._error(path, message, t));
		//log.error(message, t);
	}

	@Override
	public void close() {
	}


}
