package com.kappaware.hsync.ttools;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class MiniHdfsCluster {
	private MiniDFSCluster miniDFSCluster;
	private Configuration hdfsConfig;

	public void start(int namenodePort) throws IOException {
		File baseDir = new File("./tmp/minidfs").getAbsoluteFile();
		System.out.println(baseDir.getPath());
		FileUtil.fullyDelete(baseDir);
		
		hdfsConfig = new Configuration();
		hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*");
		hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*");
		hdfsConfig.setBoolean("dfs.permissions", false);
		hdfsConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getPath());
		
        miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig)
                .nameNodePort(namenodePort)
                .nameNodeHttpPort(0)
                .numDataNodes(1)
                .format(true)
                .racks(null)
                .build();
	}

	public void stop() {
		this.miniDFSCluster.shutdown();
	}
}
