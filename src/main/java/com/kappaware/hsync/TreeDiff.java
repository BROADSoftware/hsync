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
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Vector;

import com.kappaware.hsync.Tree.Folder;
import com.kappaware.hsync.Tree.Node;
import com.kappaware.hsync.Tree.File;

public class TreeDiff {
	
	private List<Folder> foldersToCreate = new Vector<Folder>();
	private List<Folder> foldersToAdjust = new Vector<Folder>();
	private List<File> filesToCreate = new Vector<File>();
	private List<File> filesToReplace = new Vector<File>();
	private List<File> filesToAdjust = new Vector<File>();
	private List<File> filesToDelete = new Vector<File>();	// Only xxx.tmp_hsync (Remaining from a previous crashed run)
	private Tree source;

	
	public void toYaml(Writer w) throws IOException {
		L(w, "foldersToCreate", this.foldersToCreate);
		L(w, "foldersToAdjust", this.foldersToAdjust);
		L(w, "filesToCreate", this.filesToCreate);
		L(w, "filesToReplace", this.filesToReplace);
		L(w, "filesToAdjust", this.filesToAdjust);
		L(w, "filesToDelete", this.filesToDelete);
		L(w, "excludedFolders", this.source.getExcludedFolders());
		L(w, "excludedFiles", this.source.getExcludedFiles());
	}

	
	public TreeDiff(Tree source, Tree target) {
		this.source = source;
		for(Entry<String, Folder> entry : source.folderByName.entrySet())  {
			if(target.folderByName.containsKey(entry.getKey())) {
				if(!isPermissionEquals(entry.getValue(), target.folderByName.get(entry.getKey()))) {
					this.foldersToAdjust.add(entry.getValue());
				}
			} else {
				this.foldersToCreate.add(entry.getValue());
			}
		}
		Collections.sort(this.filesToAdjust);
		Collections.sort(this.foldersToCreate);
		for(File file : target.fileByName.values()) {
			if(file.path.endsWith(Tree.TMP_EXT)) {
				this.filesToDelete.add(file);
			}
		}
		for(Entry<String, File> entry : source.fileByName.entrySet()) {
			if(target.fileByName.containsKey(entry.getKey())) {
				File src = entry.getValue();
				File tgt = target.fileByName.get(entry.getKey());
				if (src.size != tgt.size || src.modificationTime != tgt.modificationTime) {
					this.filesToReplace.add(src);
				} else if(!isPermissionEquals(src, tgt)) {
					this.filesToAdjust.add(src);
				}
			} else if (target.folderByName.containsKey(entry.getKey())) {
				// We want to copy a file with same name as an existing folder on target. Rename the existing folder
				this.filesToReplace.add(entry.getValue());
			} else {
				this.filesToCreate.add(entry.getValue());
			}
		}
	}

	private static boolean isPermissionEquals(Node x, Node y) {
		return x.getOwner().equals(y.getOwner()) && x.getGroup().equals(y.getGroup()) && x.getMode() == y.getMode(); 
	}

	private void L(Writer w, String title, List<? extends Node> nodes) throws IOException {
		w.write(title + ":");
		if (nodes == null || nodes.size() == 0) {
			w.write(" []\n");
		} else {
			w.write("\n");
			for(Node node : nodes) {
				w.write("- " + node.toYaml() + "\n");
			}
		}
	}

	
	public List<Folder> getFoldersToCreate() {
		return foldersToCreate;
	}

	public List<Folder> getFoldersToAdjust() {
		return foldersToAdjust;
	}

	public List<File> getFilesToCreate() {
		return filesToCreate;
	}

	public List<File> getFilesToReplace() {
		return filesToReplace;
	}

	public List<File> getFilesToAdjust() {
		return filesToAdjust;
	}

	public List<File> getFilesToDelete() {
		return filesToDelete;
	}

	
}
