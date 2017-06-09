package com.kappaware.hsync;

import java.io.IOException;
import java.util.Queue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hsync.notifier.Notifier;

public class FileThread extends Thread {
	static Logger log = LoggerFactory.getLogger(FileThread.class);
	
	private int slot;
	private FileSystem fileSystem;
	private String srcPath;
	private String tgtPath;
	private Queue<FileAction> queue;
	private Notifier notifier;
	
	private boolean running = true;
	
	private int fileCount = 0;
	private long volume = 0;
	

	public FileThread(int slot, FileSystem fileSystem, String srcPath, String tgtPath, Queue<FileAction> queue, Notifier notifier) {
		super();
		this.slot = slot;
		this.fileSystem = fileSystem;
		this.srcPath = srcPath;
		this.tgtPath = tgtPath;
		this.queue = queue;
		this.notifier = notifier;
		this.setDaemon(false); // This is NOT a deamon thread
		this.start();
	}

	public void stopRunning() {
		this.running = false;
	}

	@Override
	public void run() {
		while (running) {
			FileAction action = this.queue.poll();
			if (action == null) {
				this.running = false;
			} else {
				Tree.File file = action.getFile();
				try {
					switch (action.getType()) {
						case ADJUST:
							Path path = Utils.concatPath(this.tgtPath, file.path);
							fileSystem.setPermission(path, new FsPermission(file.mode));
							fileSystem.setOwner(path, file.owner, file.group);
							notifier.fileAdjusted(path, file.owner, file.group, file.mode);
						break;
						case COPY:
							this.copy(file);
						break;
						case REPLACE:
							Path target = Utils.concatPath(this.tgtPath, file.path);
							int x = 1;
							Path shadow;
							do {
								shadow = new Path(String.format("%s_%03d", target, x++));
							} while( this.fileSystem.exists(shadow));
							this.fileSystem.rename(target,  shadow);
							notifier.fileRenamed(target, shadow);
							this.copy(file);
						break;
						default:
						break;

					}
				} catch (IOException e) {
					notifier.error(new Path(file.path), "", e);
					log.error(String.format("handling of '%s'", file.path), e);
				}
			}
		}
	}
	
	private void copy(Tree.File file) throws IOException {
		Path target = Utils.concatPath(this.tgtPath, file.path);
		Path tmpTarget = Utils.concatPath(this.tgtPath, file.path + Tree.TMP_EXT);
		Path src = Utils.concatPath(this.srcPath, file.path);
		this.fileSystem.copyFromLocalFile(false, false, src, tmpTarget);
		this.fileSystem.setPermission(target, new FsPermission(file.mode));
		this.fileSystem.setOwner(target, file.owner, file.group);
		this.fileSystem.setTimes(target, file.modificationTime, file.modificationTime);
		// We notify BEFORE renaming. In case of crash, we prefer having a false notification than missing a file
		notifier.fileCopied(new Path(file.path), file.owner, file.group, file.mode, file.size, file.modificationTime);
		this.fileSystem.rename(tmpTarget, target);
		this.fileCount++;
		this.volume += file.size;
	}

	public int getFileCount() {
		return fileCount;
	}

	public long getVolume() {
		return volume;
	}

	public int getSlot() {
		return this.slot;
	}
	
}
