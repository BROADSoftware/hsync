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
package com.kappaware.hsync.notifier;

import java.util.List;

public class NotifierFanout implements Notifier {
	private List<Notifier> notifiers;

	public NotifierFanout(List<Notifier> notifiers) {
		this.notifiers = notifiers;
	}

	@Override
	public void fileRenamed(String oldName, String newName) {
		for(Notifier notifier : notifiers) {
			notifier.fileRenamed(oldName, newName);
		}
	}

	@Override
	public void folderCreated(String path, String owner, String group, short mode) {
		for(Notifier notifier : notifiers) {
			notifier.folderCreated(path, owner, group, mode);
		}
	}

	@Override
	public void folderAdjusted(String path, String owner, String group, short mode) {
		for(Notifier notifier : notifiers) {
			notifier.folderAdjusted(path, owner, group, mode);
		}
	}

	@Override
	public void fileDeleted(String path) {
		for(Notifier notifier : notifiers) {
			notifier.fileDeleted(path);
		}
	}

	@Override
	public void copyStarted(String path) {
		for(Notifier notifier : notifiers) {
			notifier.copyStarted(path);
		}
	}

	@Override
	public void fileCopied(String path, String owner, String group, short mode, long size, long modTime) {
		for(Notifier notifier : notifiers) {
			notifier.fileCopied(path, owner, group, mode, size, modTime);
		}
	}

	@Override
	public void fileAdjusted(String path, String owner, String group, short mode) {
		for(Notifier notifier : notifiers) {
			notifier.fileAdjusted(path, owner, group, mode);
		}
	}

	@Override
	public void error(String path, String message, Throwable t) {
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
