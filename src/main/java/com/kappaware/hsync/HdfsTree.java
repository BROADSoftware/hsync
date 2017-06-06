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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.kappaware.hsync.config.ConfigurationException;

public class HdfsTree extends Tree {
	FileSystem fileSystem;
	
	public HdfsTree(FileSystem fileSystem, String root) throws ConfigurationException, IOException {
		this.fileSystem = fileSystem;
		Path rootPath = new Path(root);
		this.root = Path.getPathWithoutSchemeAndAuthority(rootPath).toString();
		if(!this.fileSystem.isDirectory(rootPath)) {
			throw new ConfigurationException(String.format("HDFS path '%s' does not exists, or is not a folder", this.root));
		}
		dig(rootPath);
	}

	private void dig(Path folderPath) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fileStatuses = this.fileSystem.listStatus(folderPath);
		for(FileStatus fs : fileStatuses) {
			if(fs.isDirectory()) {
				Folder folder = new Folder(this.adjustPath(fs.getPath()), fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getModificationTime());
				this.folderByName.put(folder.path, folder);
				dig(fs.getPath());
			} else {
				File file = new File(this.adjustPath(fs.getPath()), fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getModificationTime(), fs.getLen());
				this.fileByName.put(file.path, file);
			}
		}
	}
	
	/**
	 * Adjust the path relative to our root
	 * @param path
	 * @return
	 */
	String adjustPath(Path path) {
		//return path.toString();
		Path p2 = Path.getPathWithoutSchemeAndAuthority(path);
		if("/".equals(this.root)) {
			return p2.toString();
		} else {
			return p2.toString().substring(this.root.length() + 1);
		}
	}
	
}
