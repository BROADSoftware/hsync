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
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hsync.config.ConfigurationException;

public class Tree {
	static Logger log = LoggerFactory.getLogger(Tree.class);
	static public final String TMP_EXT = "._TMP_HSYNC_";
	
	protected String root;	// Initialized by subclass
	protected Map<String, File> fileByName = new HashMap<String, File>();
	protected Map<String, Folder> folderByName = new HashMap<String, Folder>();
	protected List<File> excludedFiles = new Vector<File>();
	protected List<Folder> excludedFolders = new Vector<Folder>();
	private List<PathMatcher> excludes = new Vector<PathMatcher>();

	public Tree(List<String> excludeStrings) throws ConfigurationException, IOException {
		if (excludeStrings != null) {
			for (String s : excludeStrings) {
				this.excludes.add(FileSystems.getDefault().getPathMatcher("glob:" + s));
			}
		}
	}
	
	protected boolean isExcluded(String path) {
		java.nio.file.Path p = (new java.io.File(path)).toPath();
		for (PathMatcher pm : this.excludes) {
			if (pm.matches(p)) {
				log.debug(String.format("Node '%s' REFUSED", path));
				return true;
			}
			
		}
		log.debug(String.format("Node '%s' Accepted", path));
		return false;
	}
	
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
		sb.append("Excluded folders:\n");
		for(Folder f : this.excludedFolders) {
			sb.append("\t" + f.toString() + "\n");
		}
		sb.append("Excluded files:\n");
		for(File f : this.excludedFiles) {
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
	
	
	public List<File> getExcludedFiles() {
		return excludedFiles;
	}

	public List<Folder> getExcludedFolders() {
		return excludedFolders;
	}

	
	// ------------------------------------------------------------------------------------
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm");

	static String FD(long ts) {
		synchronized (sdf) {
			return sdf.format(new Date(ts));
		}
	}	
}
