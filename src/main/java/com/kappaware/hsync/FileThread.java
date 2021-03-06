/*
 * Copyright (C) 2017 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
	private int errorCount = 0;
	

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
							notifier.fileAdjusted(path.toString(), file.owner, file.group, file.mode);
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
							notifier.fileRenamed(target.toString(), shadow.toString());
							this.copy(file);
						break;
						default:
						break;

					}
				} catch (IOException e) {
					Path path = Utils.concatPath(this.tgtPath, file.path);
					notifier.error(path.toString(), "", e);
					log.error(String.format("handling of '%s'", path.toString()), e);
					errorCount++;
					this.running = false;
				}
			}
		}
	}
	
	private void copy(Tree.File file) throws IOException {
		Path target = Utils.concatPath(this.tgtPath, file.path);
		notifier.copyStarted(target.toString());
		Path tmpTarget = Utils.concatPath(this.tgtPath, file.path + Tree.TMP_EXT);
		Path src = Utils.concatPath(this.srcPath, file.path);
		this.fileSystem.copyFromLocalFile(false, false, src, tmpTarget);
		this.fileSystem.setPermission(tmpTarget, new FsPermission(file.mode));
		this.fileSystem.setOwner(tmpTarget, file.owner, file.group);
		this.fileSystem.setTimes(tmpTarget, file.modificationTime, -1);
		// We notify BEFORE renaming. In case of crash, we prefer having a false notification than missing a file
		notifier.fileCopied(target.toString(), file.owner, file.group, file.mode, file.size, file.modificationTime);
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
	public int getErrorCount() {
		return this.errorCount;
	}
}
