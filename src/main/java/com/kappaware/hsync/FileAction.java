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
