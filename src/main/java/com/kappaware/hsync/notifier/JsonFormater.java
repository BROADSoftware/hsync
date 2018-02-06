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


public class JsonFormater {
	private String clientId;
	
	public JsonFormater(String clientId) {
		this.clientId = clientId;
	}

	public String _fileRenamed(String oldName, String newName) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"FILE_RENAMED\", \"oldName\":\"%s\", \"newName\":\"%s\" }", this.clientId, oldName, newName);
	}

	public String _folderCreated(String path, String owner, String group, short mode) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"FOLDER_CREATED\", \"path\":\"%s\", \"owner\":\"%s\", \"group\":\"%s\", \"mode\":%d }", this.clientId, path, owner, group, mode);
	}

	public String _folderAdjusted(String path, String owner, String group, short mode) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"FOLDER_ADJUSTED\", \"path\":\"%s\", \"owner\":\"%s\", \"group\":\"%s\", \"mode\":%d }", this.clientId, path, owner, group, mode);
	}

	public String _fileDeleted(String path) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"FILE_DELETED\", \"path\":\"%s\" }", this.clientId, path);
	}

	public String _copyStarted(String path) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"COPY_STARTED\", \"path\":\"%s\" }", this.clientId, path);
	}

	public String _fileCopied(String path, String owner, String group, short mode, long size, long modTime) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"FILE_COPIED\", \"path\":\"%s\", \"owner\":\"%s\", \"group\":\"%s\", \"mode\":%d, \"size\":%d, \"modificationTime\":%d }", this.clientId, path, owner, group, mode, size, modTime);
	}

	public String _fileAdjusted(String path, String owner, String group, short mode) {
		return String.format("{ \"clientId\":\"%s\", \"action\":\"FILE_ADJUSTED\", \"path\":\"%s\", \"owner\":\"%s\", \"group\":\"%s\", \"mode\":%d }", this.clientId, path, owner, group, mode);
	}

	public String _error(String path, String message, Throwable t) {
		return String.format("{ \"clientId\": \"%s\", \"action\":\"ERROR\", \"path\":\"%s\", \"message\":\"%s\", \"exception\":\"%s\" }", this.clientId, path, message, t.toString());
		//log.error(message, t);
	}


}
