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


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Tree {
	
	static public class Folder {
		public String path;
		public String owner;
		public String group;
		public int mode;
		
		public Folder(String path, String owner, String group, int mode) {
			this.path = path;
			this.owner = owner;
			this.group = group;
			this.mode = mode;
		}


		@Override
		public String toString() {
			return String.format("%s  owner:%s  group:%s  mode:%04o ", path, owner, group, mode);
		}
	}
	
	static public class File {
		public String path;
		public String owner;
		public String group;
		public int mode;
		public long modificationTime;
		public long size;

		public File(String path, String owner, String group, int mode, long modificationTime, long size) {
			this.path = path;
			this.owner = owner;
			this.group = group;
			this.mode = mode;
			this.modificationTime = modificationTime;
			this.size = size;
		}

		@Override
		public String toString() {
			return String.format("%s  owner:%s  group:%s  mode:%04o modTime:%d  size:%d", path, owner, group, mode, this.modificationTime, size);
		}
	}
	public String root;
	public List<Folder> folders = new Vector<Folder>();
	public List<File> files = new Vector<File>();
	public Map<String, File> fileByName = new HashMap<String, File>();
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("ROOT: " + this.root + "\nFolders:\n");
		for(Folder f : folders) {
			sb.append("\t" + f.toString() + "\n");
		}
		sb.append("Files:\n");
		for(File f : files) {
			sb.append("\t" + f.toString() + "\n");
		}
		return sb.toString();
	}
	
	
}
