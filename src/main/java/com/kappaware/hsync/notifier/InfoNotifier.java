package com.kappaware.hsync.notifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InfoNotifier extends StringFormater implements Notifier {
	static Logger log = LoggerFactory.getLogger(InfoNotifier.class);
	
	public InfoNotifier(String clientId) {
		super(clientId);
	}

	@Override
	public void fileRenamed(String oldName, String newName) {
		log.info(this._fileRenamed(oldName, newName));
	}

	@Override
	public void folderCreated(String path, String owner, String group, short mode) {
		log.info(this._folderCreated(path, owner, group, mode));
	}

	@Override
	public void folderAdjusted(String path, String owner, String group, short mode) {
		log.info(this._folderAdjusted(path, owner, group, mode));
	}

	@Override
	public void fileDeleted(String path) {
		log.info(this._fileDeleted(path));
	}

	@Override
	public void copyStarted(String path) {
		log.info(this._copyStarted(path));
	}

	@Override
	public void fileCopied(String path, String owner, String group, short mode, long size, long modTime) {
		log.info(this._fileCopied(path, owner, group, mode, size, modTime));
	}
	
	@Override
	public void fileAdjusted(String path, String owner, String group, short mode) {
		log.info(this._fileAdjusted(path, owner, group, mode));
	}

	@Override
	public void error(String path, String message, Throwable t) {
		log.info(this._error(path, message, t));
		//log.error(message, t);
	}

	@Override
	public void close() {
	}


}
