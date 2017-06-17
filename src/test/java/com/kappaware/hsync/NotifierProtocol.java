package com.kappaware.hsync;

public class NotifierProtocol {
	public String clientId;
	public String action;

	static public class FILE_RENAMED extends NotifierProtocol {
		public String oldName;
		public String newName;
	}
	
	static public class FOLDER_CREATED extends NotifierProtocol {
		public String path;
		public String owner;
		public String group;
		public short mode;
	}
	
	static public class FOLDER_ADJUSTED extends NotifierProtocol {
		public String path;
		public String owner;
		public String group;
		public short mode;
	}

	static public class FILE_DELETED extends NotifierProtocol {
		public String path;
	}

	static public class COPY_STARTED extends NotifierProtocol {
		public String path;
	}

	static public class FILE_COPIED extends NotifierProtocol {
		public String path;
		public String owner;
		public String group;
		public short mode;
		public long size;
		public long modificationTime;
	}

	static public class FILE_ADJUSTED extends NotifierProtocol {
		public String path;
		public String owner;
		public String group;
		public short mode;
	}

}
