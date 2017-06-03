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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.kappaware.hsync.config.ConfigurationException;

public class HdfsTree extends Tree {
	
	public HdfsTree(FileSystem fs, String rootPath) throws ConfigurationException, IOException {
		this.root = rootPath;
		Path path = new Path(this.root);
		if(!fs.isDirectory(path)) {
			throw new ConfigurationException(String.format("HDFS path '%s' does not exists, or is not a folder", this.root));
		}
		RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
		while(it.hasNext()) {
			LocatedFileStatus fileStatus = it.next();
			System.out.println(fileStatus.toString());
		}
		
	}

}
