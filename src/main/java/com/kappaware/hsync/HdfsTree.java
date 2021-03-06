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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.kappaware.hsync.config.ConfigurationException;

public class HdfsTree extends Tree {
	FileSystem fileSystem;

	public HdfsTree(FileSystem fileSystem, String root, List<String> excludeStrings) throws ConfigurationException, IOException {
		super(excludeStrings);
		this.fileSystem = fileSystem;
		Path rootPath = new Path(root);
		this.root = Path.getPathWithoutSchemeAndAuthority(rootPath).toString();
		if (!this.fileSystem.isDirectory(rootPath)) {
			throw new ConfigurationException(String.format("HDFS path '%s' does not exists, or is not a folder", this.root));
		}
		dig(rootPath);
		Collections.sort(this.excludedFiles);
		Collections.sort(this.excludedFolders);
	}

	/*
	 * TODO: Currently, exclusion is not tested on HdfsTree
	 */
	private void dig(Path folderPath) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fileStatuses = this.fileSystem.listStatus(folderPath);
		for (FileStatus fs : fileStatuses) {
			if (fs.isDirectory()) {
				String adjustedPath = this.adjustPath(fs.getPath());
				Folder folder = new Folder(adjustedPath, fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getModificationTime());
				if (this.isExcluded(adjustedPath)) {
					this.excludedFolders.add(folder);
				} else {
					this.folderByName.put(folder.path, folder);
					dig(fs.getPath());
				}
			} else {
				String adjustedPath = this.adjustPath(fs.getPath());
				File file = new File(adjustedPath, fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getModificationTime(), fs.getLen());
				if (this.isExcluded(adjustedPath)) {
					this.excludedFiles.add(file);
				} else {
					this.fileByName.put(file.path, file);
				}
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
		if ("/".equals(this.root)) {
			return p2.toString();
		} else {
			return "." + p2.toString().substring(this.root.length());
		}
	}

}
