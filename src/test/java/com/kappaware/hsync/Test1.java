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

import com.kappaware.hsync.Ls.Folder;
import com.kappaware.hsync.config.ConfigurationException;

public class Test1 {
	static MiniHdfsCluster cluster;

	@BeforeClass
	public static void setup() throws Exception {
		cluster = new MiniHdfsCluster();
		cluster.start(8020);
		FileSystem fs = FileSystem.get(new Configuration());
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
	public void test01Reporting() throws IOException, ConfigurationException {
		String lp = (new File("src/test/resources/test01")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test01", "--reportFile", "./tmp/report01.yml", "--dryRun" };
		Main.main2(argv);
		File fr = new File("./tmp/report01.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(2, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(3, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToDelete").size());
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "folder1"));
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "folder1/subfolder1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "folder1/subfolder1/subfile1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "folder1/File1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "file0"));
	}


	@Test
	public void test02Delete() throws IOException, ConfigurationException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(new Path("src/test/resources/test02/file1.txt"), new Path("/tests/test02/file0.txt.tmp_hsync"));
		//System.out.println(Ls.hdfs("/tests").toString());
		String lp = (new File("src/test/resources/test02")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test02", "--reportFile", "./tmp/report02.yml" };
		Main.main2(argv);
		File fr = new File("./tmp/report02.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(0, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(1, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(1, report.getList("filesToDelete").size());
		Assert.assertNotNull(report.findPathInList("filesToCreate", "file1.txt"));
		Assert.assertNotNull(report.findPathInList("filesToDelete", "file0.txt.tmp_hsync"));
		Ls ls = Ls.hdfs("/tests/test02");
		//System.out.println(ls.toString());
		Assert.assertEquals(0, ls.size());
	}


	@Test
	public void test03AdjustFolder() throws IOException, ConfigurationException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.mkdirs(new Path("/tests/test03/"));
		fs.mkdirs(new Path("/tests/test03/folder1"));
		fs.mkdirs(new Path("/tests/test03/folder1/subfolder1"));
		//Ls ls = Ls.hdfs("/tests/test03");  System.out.println(ls.toString());
		String lp = (new File("src/test/resources/test03")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test03", "--reportFile", "./tmp/report03.yml", "--owner", "hdfs", "--group", "hadoop", "--folderMode", "0700" };
		Main.main2(argv);
		File fr = new File("./tmp/report03.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(0, report.getList("foldersToCreate").size());
		Assert.assertEquals(2, report.getList("foldersToAdjust").size());
		Assert.assertEquals(1, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToDelete").size());
		Assert.assertNotNull(report.findPathInList("foldersToAdjust", "folder1"));
		Assert.assertNotNull(report.findPathInList("foldersToAdjust", "folder1/subfolder1"));
		Ls ls2 = Ls.hdfs("/tests/test03");
		//System.out.println(ls2.toString());
		Ls.Folder node = (Folder) ls2.getByPath("folder1");
		Assert.assertNotNull(node);
		Assert.assertEquals("hdfs", node.owner);
		Assert.assertEquals("hadoop", node.group);
		Assert.assertEquals(0700, node.mode);
	}

	@Test
	public void test04CreateFolder() throws IOException, ConfigurationException {
		String lp = (new File("src/test/resources/test04")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test04", "--reportFile", "./tmp/report04.yml", "--owner", "hdfs", "--group", "hadoop", "--folderMode", "0700" };
		Main.main2(argv);
		File fr = new File("./tmp/report04.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(2, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(1, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToDelete").size());
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "folder1"));
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "folder1/subfolder1"));
		Ls ls2 = Ls.hdfs("/tests/test04");
		//System.out.println(ls2.toString());
		Ls.Folder node = (Folder) ls2.getByPath("folder1");
		Assert.assertNotNull(node);
		Assert.assertEquals("hdfs", node.owner);
		Assert.assertEquals("hadoop", node.group);
		Assert.assertEquals(0700, node.mode);
		Ls.Folder node2 = (Folder) ls2.getByPath("folder1/subfolder1");
		Assert.assertNotNull(node2);
		Assert.assertEquals("hdfs", node2.owner);
		Assert.assertEquals("hadoop", node2.group);
		Assert.assertEquals(0700, node2.mode);
	}
}
