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


/**
 * All path are relative to targetRoot
 * @author sa
 *
 */
public interface Notifier {
	
	void fileRenamed(String oldName, String newName);

	void folderCreated(String path, String owner, String group, short mode);

	void folderAdjusted(String path, String owner, String group, short mode);

	void fileDeleted(String path);
	
	void copyStarted(String path);

	void fileCopied(String path, String owner, String group, short mode, long size, long modTime);

	void fileAdjusted(String path, String owner, String group, short mode);
	
	void error(String path, String message, Throwable t);

	void close();
}
