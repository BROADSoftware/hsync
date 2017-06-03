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

import java.util.List;
import java.util.Map;

public class Tree {
	
	static public class Folder {
		public String path;
		public String owner;
		public String group;
		public int mode;
	}
	
	static public class File {
		public String path;
		public String owner;
		public String group;
		public int mode;
		public long modificationTime;
		public long size;
	}
	public String root;
	public List<Folder> folders;
	public List<File> files;
	public Map<String, File> fileByName;
	
}
