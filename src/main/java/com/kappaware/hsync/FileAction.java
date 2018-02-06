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

import com.kappaware.hsync.Tree.File;

public class FileAction {
	
	public enum Type {
		COPY,
		ADJUST,
		REPLACE
	}
	
	private Type type;
	private File file;
	
	public FileAction(Type type, File file) {
		this.type = type;
		this.file = file;
	}

	public Type getType() {
		return type;
	}

	public File getFile() {
		return file;
	}

}
