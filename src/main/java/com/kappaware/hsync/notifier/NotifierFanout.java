package com.kappaware.hsync.notifier;

import java.util.List;

import org.apache.hadoop.fs.Path;

public class NotifierFanout implements Notifier {
	private List<Notifier> notifiers;

	public NotifierFanout(List<Notifier> notifiers) {
		this.notifiers = notifiers;
	}

	@Override
	public void fileRenamed(Path oldName, Path newName) {
		for(Notifier notifier : notifiers) {
			notifier.fileRenamed(oldName, newName);
		}
	}

	@Override
	public void folderCreated(Path path, String owner, String group, short mode) {
		for(Notifier notifier : notifiers) {
			notifier.folderCreated(path, owner, group, mode);
		}
	}

	@Override
	public void folderAdjusted(Path path, String owner, String group, short mode) {
		for(Notifier notifier : notifiers) {
			notifier.folderAdjusted(path, owner, group, mode);
		}
	}

	@Override
	public void fileDeleted(Path path) {
		for(Notifier notifier : notifiers) {
			notifier.fileDeleted(path);
		}
	}

	@Override
	public void startFileCopy(Path path) {
		for(Notifier notifier : notifiers) {
			notifier.startFileCopy(path);
		}
	}

	@Override
	public void fileCopied(Path path, String owner, String group, short mode, long size, long modTime) {
		for(Notifier notifier : notifiers) {
			notifier.fileCopied(path, owner, group, mode, size, modTime);
		}
	}

	@Override
	public void fileAdjusted(Path path, String owner, String group, short mode) {
		for(Notifier notifier : notifiers) {
			notifier.fileAdjusted(path, owner, group, mode);
		}
	}

	@Override
	public void error(Path path, String message, Throwable t) {
		for(Notifier notifier : notifiers) {
			notifier.error(path, message, t);
		}
	}

	@Override
	public void close() {
		for(Notifier notifier : notifiers) {
			notifier.close();
		}
	}

}
