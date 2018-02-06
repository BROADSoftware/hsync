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

import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.kappaware.hsync.config.ConfigurationException;

/*
 * https://stackoverflow.com/questions/13241967/change-file-owner-group-under-linux-with-java-nio-files
 */

public class LocalTree extends Tree {

	public LocalTree(String root, List<String> excludeStrings) throws ConfigurationException, IOException {
		super(excludeStrings);
		java.io.File f = new java.io.File(root);
		this.root = f.getAbsolutePath();
		if (!f.isDirectory()) {
			throw new ConfigurationException(String.format("Local path '%s' does not exists, or is not a folder", this.root));
		}
		dig(f);
		Collections.sort(this.excludedFiles);
		Collections.sort(this.excludedFolders);
	}

	private void dig(java.io.File f) throws IOException {
		java.io.File[] files = f.listFiles((FileFilter) null);
		for (java.io.File file : files) {
			if (file.isDirectory()) {
				Path path = file.toPath();
				String adjustedPath = this.adjustPath(file);
				Folder folder = new Folder(adjustedPath, getOwner(path), getGroup(path), getMode(path), getLastModificationTime(path));
				if (this.isExcluded(adjustedPath)) {
					this.excludedFolders.add(folder);
				} else {
					this.folderByName.put(folder.path, folder);
					dig(file);
				}
			} else {
				Path path = file.toPath();
				String adjustedPath = this.adjustPath(file);
				File fl = new File(adjustedPath, getOwner(path), getGroup(path), getMode(path), getLastModificationTime(path), Files.size(path));
				if (this.isExcluded(adjustedPath)) {
					this.excludedFiles.add(fl);
				} else {
					this.fileByName.put(fl.path, fl);
				}
			}
		}
	}

	private String adjustPath(java.io.File file) {
		if("/".equals(this.root)) {
			return file.getAbsolutePath();
		} else {
			return "." + file.getAbsolutePath().substring(this.root.length());
		}
	}

	private static String getOwner(Path path) throws IOException {
		return Files.getOwner(path).getName();
	}

	private static String getGroup(Path path) throws IOException {
		return Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS).group().getName();
	}

	private static short getMode(Path path) throws IOException {
		Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path);
		short mode = 0;
		for (PosixFilePermission perm : perms) {
			switch (perm) {
				case GROUP_EXECUTE:
					mode += 0010;
				break;
				case GROUP_READ:
					mode += 0040;
				break;
				case GROUP_WRITE:
					mode += 0020;
				break;
				case OTHERS_EXECUTE:
					mode += 0001;
				break;
				case OTHERS_READ:
					mode += 0004;
				break;
				case OTHERS_WRITE:
					mode += 0002;
				break;
				case OWNER_EXECUTE:
					mode += 0100;
				break;
				case OWNER_READ:
					mode += 0400;
				break;
				case OWNER_WRITE:
					mode += 0200;
				break;
				default:
				break;

			}
		}
		return mode;
	}

	private static long getLastModificationTime(Path path) throws IOException {
		return Files.getLastModifiedTime(path, LinkOption.NOFOLLOW_LINKS).toMillis();
	}

}
