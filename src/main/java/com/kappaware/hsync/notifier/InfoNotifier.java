package com.kappaware.hsync.notifier;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InfoNotifier extends NotifierFormater implements Notifier {
	static Logger log = LoggerFactory.getLogger(InfoNotifier.class);
	
	public InfoNotifier(Path root, String clientId) {
		super(root, clientId);
	}

	@Override
	public void fileRenamed(Path oldName, Path newName) {
		log.info(this._fileRenamed(oldName, newName));
	}

	@Override
	public void folderCreated(Path path, String owner, String group, short mode) {
		log.info(this._folderCreated(path, owner, group, mode));
	}

	@Override
	public void folderAdjusted(Path path, String owner, String group, short mode) {
		log.info(this._folderAdjusted(path, owner, group, mode));
	}

	@Override
	public void fileDeleted(Path path) {
		log.info(this._fileDeleted(path));
	}

	@Override
	public void startFileCopy(Path path) {
		log.info(this._startFileCopy(path));
	}

	@Override
	public void fileCopied(Path path, String owner, String group, short mode, long size, long modTime) {
		log.info(this._fileCopied(path, owner, group, mode, size, modTime));
	}
	
	@Override
	public void fileAdjusted(Path path, String owner, String group, short mode) {
		log.info(this._fileAdjusted(path, owner, group, mode));
	}

	@Override
	public void error(Path path, String message, Throwable t) {
		log.info(this._error(path, message, t));
		//log.error(message, t);
	}


}
