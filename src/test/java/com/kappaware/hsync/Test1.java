package com.kappaware.hsync;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kappaware.hsync.config.ConfigurationException;

public class Test1 {
	static MiniHdfsCluster cluster;

	@BeforeClass
	public static void setup() throws Exception {
		cluster = new MiniHdfsCluster();
		cluster.start(8020);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.mkdirs(new Path("/tests"));
		fs.mkdirs(new Path("/tests/test00"));
		fs.mkdirs(new Path("/tests/test01"));
		fs.mkdirs(new Path("/tests/test02"));
		fs.mkdirs(new Path("/tests/test03"));
		fs.mkdirs(new Path("/tests/test04"));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		cluster.stop();
	}



	@Test
	public void test01() throws IOException, ConfigurationException {
		String lp = (new File("src/test/resources/test01")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test01", "--reportFile", "./tmp/report01.yml" };
		Main.main2(argv);
		File fr = new File("./tmp/report01.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(1, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(2, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToDelete").size());
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "folder1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "folder1/File1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "file0"));
	}
	

	
}
