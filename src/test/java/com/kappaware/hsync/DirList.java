package com.kappaware.hsync;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DirList {
	private String root;
	private List<Node> nodes = new Vector<Node>();
	private Map<String, Node> nodeByPath = new HashMap<String, Node>();
	private FileSystem fileSystem;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm");

	static String FD(long ts) {
		synchronized (sdf) {
			return sdf.format(new Date(ts));
		}
	}

	public DirList(String root) throws IOException {
		this(FileSystem.get(new Configuration()), root);
	}

	public DirList(FileSystem fileSystem, String root) throws IOException {
		this.fileSystem = fileSystem;
		Path rootPath = new Path(root);
		this.root = Path.getPathWithoutSchemeAndAuthority(rootPath).toString();
		if (!this.fileSystem.isDirectory(rootPath)) {
			throw new IOException(String.format("HDFS path '%s' does not exists, or is not a folder", this.root));
		}
		dig(rootPath);
		Collections.sort(nodes);
	}

	private void dig(Path folderPath) throws FileNotFoundException, IllegalArgumentException, IOException {
		FileStatus[] fileStatuses = this.fileSystem.listStatus(folderPath);
		for (FileStatus fs : fileStatuses) {
			if (fs.isDirectory()) {
				String path = this.adjustPath(fs.getPath());
				Folder folder = new Folder(path, fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getModificationTime());
				this.nodes.add(folder);
				this.nodeByPath.put(path, folder);
				dig(fs.getPath());
			} else {
				String path = this.adjustPath(fs.getPath());
				File file = new File(path, fs.getOwner(), fs.getGroup(), fs.getPermission().toShort(), fs.getModificationTime(), fs.getLen());
				this.nodes.add(file);
				this.nodeByPath.put(path, file);
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

	public interface Node extends Comparable<Node> {
		public String getPath();

	}

	static public class Folder implements Node {
		public String path;
		public String owner;
		public String group;
		public int mode;
		public long modificationTime;

		public Folder(String path, String owner, String group, int mode, long modificationTime) {
			this.path = path;
			this.owner = owner;
			this.group = group;
			this.mode = mode;
			this.modificationTime = modificationTime;
		}

		@Override
		public int compareTo(Node o) {
			return this.getPath().compareTo(o.getPath());
		}

		@Override
		public String getPath() {
			return this.path;
		}

		@Override
		public String toString() {
			return String.format("[%04o] %-7s %-7s %8d %s %s\n", this.mode, this.owner, this.group, 0, FD(this.modificationTime) , this.path);
		}
	}

	static public class File implements Node {
		public String path;
		public String owner;
		public String group;
		public int mode;
		public long modificationTime;
		public long size;

		public File(String path, String owner, String group, int mode, long modificationTime, long size) {
			this.path = path;
			this.owner = owner;
			this.group = group;
			this.mode = mode;
			this.modificationTime = modificationTime;
			this.size = size;
		}

		@Override
		public String getPath() {
			return this.path;
		}

		@Override
		public int compareTo(Node o) {
			return this.getPath().compareTo(o.getPath());
		}

		@Override
		public String toString() {
			return String.format(" %04o  %-7s %-7s %8d %s %s\n", this.mode, this.owner, this.group, this.size,  FD(this.modificationTime), this.path);
		}
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(String.format("ROOT: '%s'\n", this.root));
		for (Node node : this.nodes) {
			sb.append(node.toString());
		}
		return sb.toString();
	}
	
	public Node getByPath(String path) {
		return this.nodeByPath.get(path);
	}

}
