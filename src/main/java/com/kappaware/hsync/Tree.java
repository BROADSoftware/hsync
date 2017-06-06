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


import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class Tree {
	
	public interface Node {
		String getOwner();
		String getGroup();
		int getMode();
		String toYaml();
	}
	
	static public class Folder implements Comparable<Folder>, Node {
		public String path;
		public String owner;
		public String group;
		public short mode;
		public long modificationTime;
		
		public Folder(String path, String owner, String group, short mode, long modificationTime) {
			this.path = path;
			this.owner = owner;
			this.group = group;
			this.mode = mode;
			this.modificationTime = modificationTime;
			
		}

		@Override
		public int compareTo(Folder o) {
			return this.path.compareTo(o.path);
		}

		@Override
		public String toString() {
			return String.format("[%04o] %-7s %-7s %8d %s %s", this.mode, this.owner, this.group, 0, FD(this.modificationTime) , this.path);
			//return String.format("%s  owner:%s  group:%s  mode:%04o ", path, owner, group, mode);
		}

		@Override
		public String toYaml() {
			return String.format("{ path: \"%s\",  owner: \"%s\",  group: \"%s\",  mode: %04o }", path, owner, group, mode);
		}
		
		@Override
		public String getOwner() {
			return this.owner;
		}
		@Override
		public String getGroup() {
			return this.group;
		}

		@Override
		public int getMode() {
			return this.mode;
		}

	}
	
	static public class File implements Comparable<File>, Node {
		public String path;
		public String owner;
		public String group;
		public short mode;
		public long modificationTime;
		public long size;

		public File(String path, String owner, String group, short mode, long modificationTime, long size) {
			this.path = path;
			this.owner = owner;
			this.group = group;
			this.mode = mode;
			this.modificationTime = modificationTime;
			this.size = size;
		}

		@Override
		public String toString() {
			return String.format(" %04o  %-7s %-7s %8d %s %s", this.mode, this.owner, this.group, this.size,  FD(this.modificationTime), this.path);
			//return String.format("%s  owner:%s  group:%s  mode:%04o modTime:%d  size:%d", path, owner, group, mode, this.modificationTime, size);
		}

		@Override
		public String toYaml() {
			return String.format("{ path: \"%s\",  owner: \"%s\",  group: \"%s\",  mode: %04o, modTime: \"%s\",  size: %d }", path, owner, group, mode, Utils.printIsoDateTime(this.modificationTime), size);
		}

		@Override
		public int compareTo(File o) {
			return this.path.compareTo(o.path);
		}

		@Override
		public String getOwner() {
			return this.owner;
		}
		@Override
		public String getGroup() {
			return this.group;
		}
		@Override
		public int getMode() {
			return this.mode;
		}

	}
	public String root;
	public Map<String, File> fileByName = new HashMap<String, File>();
	public Map<String, Folder> folderByName = new HashMap<String, Folder>();
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(this.root + "  ----v\nFolders:\n");
		List<Folder> folders = new Vector<Folder>(this.folderByName.values());
		Collections.sort(folders);
		for(Folder f : folders) {
			sb.append("\t" + f.toString() + "\n");
		}
		List<File> files = new Vector<File>(this.fileByName.values());
		Collections.sort(files);
		sb.append("Files:\n");
		for(File f : files) {
			sb.append("\t" + f.toString() + "\n");
		}
		return sb.toString();
	}
	
	public void adjustPermissions(String owner, String group, Short fileMode, Short folderMode) {
		if(owner != null || group != null || folderMode != null) {
			for(Folder f : this.folderByName.values()) {
				if(owner != null) {
					f.owner = owner;
				}
				if(group != null) {
					f.group = group;
				}
				if(folderMode != null) {
					f.mode = folderMode;
				}
			}
		}
		if(owner != null || group != null || fileMode != null) {
			for(File f : this.fileByName.values()) {
				if(owner != null) {
					f.owner = owner;
				}
				if(group != null) {
					f.group = group;
				}
				if(fileMode != null) {
					f.mode = fileMode;
				}
			}
		}
	}
	
	// ------------------------------------------------------------------------------------
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm");

	static String FD(long ts) {
		synchronized (sdf) {
			return sdf.format(new Date(ts));
		}
	}	
}
