package com.kappaware.hsync;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.kappaware.hsync.config.ConfigurationException;
import com.kappaware.hsync.ttools.Ls;
import com.kappaware.hsync.ttools.MiniHdfsCluster;
import com.kappaware.hsync.ttools.Report;
import com.kappaware.hsync.ttools.YamlUtils;
import com.kappaware.hsync.ttools.Ls.Folder;

public class TestBase {
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
		fs.mkdirs(new Path("/tests/test05"));
		fs.mkdirs(new Path("/tests/test06"));
		fs.mkdirs(new Path("/tests/test07"));
		fs.mkdirs(new Path("/tests/test08"));
		fs.mkdirs(new Path("/tests/test09"));
		fs.mkdirs(new Path("/tests/test10"));
		fs.mkdirs(new Path("/tests/test11"));
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
		Assert.assertEquals(0, report.getList("excludedFolders").size());
		Assert.assertEquals(0, report.getList("excludedFiles").size());
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "./folder1"));
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "./folder1/subfolder1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "./folder1/subfolder1/subfile1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "./folder1/File1"));
		Assert.assertNotNull(report.findPathInList("filesToCreate", "./file0"));
	}

	@Test
	public void test02Delete() throws IOException, ConfigurationException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(new Path("src/test/resources/test02/file1.txt"), new Path("/tests/test02/file0.txt.tmp_hsync"));
		//System.out.println(Ls.hdfs("/tests").toString());
		// Need to create an empty folder
		String lp = (new File("src/test/resources/test02b")).getAbsolutePath();
		new File(lp).mkdir();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test02", "--reportFile", "./tmp/report02.yml" };
		Main.main2(argv);
		File fr = new File("./tmp/report02.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(0, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(1, report.getList("filesToDelete").size());
		Assert.assertEquals(0, report.getList("excludedFolders").size());
		Assert.assertEquals(0, report.getList("excludedFiles").size());
		Assert.assertNotNull(report.findPathInList("filesToDelete", "./file0.txt.tmp_hsync"));
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
		Assert.assertEquals(0, report.getList("excludedFolders").size());
		Assert.assertEquals(0, report.getList("excludedFiles").size());
		Assert.assertNotNull(report.findPathInList("foldersToAdjust", "./folder1"));
		Assert.assertNotNull(report.findPathInList("foldersToAdjust", "./folder1/subfolder1"));
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
		Assert.assertEquals(0, report.getList("excludedFolders").size());
		Assert.assertEquals(0, report.getList("excludedFiles").size());
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "./folder1"));
		Assert.assertNotNull(report.findPathInList("foldersToCreate", "./folder1/subfolder1"));
		Ls lsHdfs = Ls.hdfs("/tests/test04");
		//System.out.println(ls2.toString());
		Ls.Folder node = (Folder) lsHdfs.getByPath("folder1");
		Assert.assertNotNull(node);
		Assert.assertEquals("hdfs", node.owner);
		Assert.assertEquals("hadoop", node.group);
		Assert.assertEquals(0700, node.mode);
		Ls.Folder node2 = (Folder) lsHdfs.getByPath("folder1/subfolder1");
		Assert.assertNotNull(node2);
		Assert.assertEquals("hdfs", node2.owner);
		Assert.assertEquals("hadoop", node2.group);
		Assert.assertEquals(0700, node2.mode);
		
		Ls lsLocal = Ls.local(lp);
		Assert.assertEquals(lsLocal, lsHdfs);
	}

	@Test
	public void test05CopyFile() throws ConfigurationException, IOException {
		String lp = (new File("src/test/resources/test05")).getAbsolutePath();
		Ls local = Ls.local(lp);
		//System.out.println(local.toString());
		Ls.File localNode = (Ls.File) local.getByPath("file1.txt");
		Assert.assertNotNull(localNode);
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test05", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600" };
		Main.main2(argv);
		Ls lsHdfs = Ls.hdfs("/tests/test05");
		//System.out.println(ls2.toString());
		Ls.File node = (Ls.File) lsHdfs.getByPath("file1.txt");
		Assert.assertNotNull(node);
		Assert.assertEquals("hdfs", node.owner);
		Assert.assertEquals("hadoop", node.group);
		Assert.assertEquals(0600, node.mode);
		Assert.assertEquals(localNode.size, node.size);
		Assert.assertEquals(localNode.modificationTime, node.modificationTime);
		this.checkNothingToDo(argv, "./tmp/report05.yml");

		Ls lsLocal = Ls.local(lp);
		Assert.assertEquals(lsLocal, lsHdfs);
	}

	@Test
	public void test06CopyFileWithSubdir() throws ConfigurationException, IOException {
		String lp = (new File("src/test/resources/test06")).getAbsolutePath();
		Ls local = Ls.local(lp);
		//System.out.println(local.toString());
		Ls.File localNode = (Ls.File) local.getByPath("folder1/subfolder1/file1.txt");
		Assert.assertNotNull(localNode);
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test06", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600" };
		Main.main2(argv);
		Ls lsHdfs = Ls.hdfs("/tests/test06");
		//System.out.println(ls2.toString());
		Ls.File node = (Ls.File) lsHdfs.getByPath("folder1/subfolder1/file1.txt");
		Assert.assertNotNull(node);
		Assert.assertEquals("hdfs", node.owner);
		Assert.assertEquals("hadoop", node.group);
		Assert.assertEquals(0600, node.mode);
		Assert.assertEquals(localNode.size, node.size);
		Assert.assertEquals(localNode.modificationTime, node.modificationTime);
		this.checkNothingToDo(argv, "./tmp/report06.yml");
	
		Ls lsLocal = Ls.local(lp);
		Assert.assertEquals(lsLocal, lsHdfs);
	}
	
	

	/**
	 * test07/a/file1.txt and /test07/b/file1.txt differs by modificationTime
	 * test07/b/file1.txt and /test07/c/file1.txt differs by content and size
	 */
	@Test
	public void test07ReplaceFile() throws ConfigurationException, IOException {
		String lp = (new File("src/test/resources/test07")).getAbsolutePath();
		Ls local = Ls.local(lp);
		long now = System.currentTimeMillis();
		local.getFileSystem().setTimes(Utils.concatPath(lp, "a/file1.txt"), (now - 3600000), -1);
		local.getFileSystem().setTimes(Utils.concatPath(lp, "b/file1.txt"), (now), -1);
		local.getFileSystem().setTimes(Utils.concatPath(lp, "c/file1.txt"), (now), -1);
		{
			String[] argv1 = new String[] { "--localPath", Utils.concatPath(lp, "a").toString(), "--hdfsPath", "/tests/test07", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600" };
			Main.main2(argv1);
			this.checkNothingToDo(argv1, "./tmp/report07a.yml");
		}
		{
			String[] argv2 = new String[] { "--localPath", Utils.concatPath(lp, "b").toString(), "--hdfsPath", "/tests/test07", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600" };
			Main.main2(argv2);
			this.checkNothingToDo(argv2, "./tmp/report07b.yml");
			Ls lsHdfs = Ls.hdfs("/tests/test07");
			//System.out.println(lsHdfs.toString());
			Ls.File f1 = (com.kappaware.hsync.ttools.Ls.File) lsHdfs.getByPath("file1.txt");
			Assert.assertNotNull(f1);
			Assert.assertEquals(now / 1000, f1.modificationTime / 1000);
			Assert.assertEquals(((Ls.File) local.getByPath("b/file1.txt")).size, f1.size);

			Ls.File f1_001 = (com.kappaware.hsync.ttools.Ls.File) lsHdfs.getByPath("file1.txt_001");
			Assert.assertNotNull(f1_001);
			Assert.assertEquals((now - 3600000) / 1000, f1_001.modificationTime / 1000);
			Assert.assertEquals(((Ls.File) local.getByPath("a/file1.txt")).size, f1_001.size);
			
			Ls lsLocal = Ls.local(Utils.concatPath(lp, "b").toString());
			Assert.assertNotEquals(lsLocal, lsHdfs);		// Differs as there is _001

		}

		{
			String[] argv3 = new String[] { "--localPath", Utils.concatPath(lp, "c").toString(), "--hdfsPath", "/tests/test07", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600" };
			Main.main2(argv3);
			this.checkNothingToDo(argv3, "./tmp/report07c.yml");
			Ls lsHdfs = Ls.hdfs("/tests/test07");
			//System.out.println(lsHdfs.toString());
			Ls.File f1 = (com.kappaware.hsync.ttools.Ls.File) lsHdfs.getByPath("file1.txt");
			Assert.assertNotNull(f1);
			Assert.assertEquals(now / 1000, f1.modificationTime / 1000);
			Assert.assertEquals(((Ls.File) local.getByPath("c/file1.txt")).size, f1.size);

			Ls.File f1_001 = (com.kappaware.hsync.ttools.Ls.File) lsHdfs.getByPath("file1.txt_001");
			Assert.assertNotNull(f1_001);
			Assert.assertEquals((now - 3600000) / 1000, f1_001.modificationTime / 1000);
			Assert.assertEquals(((Ls.File) local.getByPath("a/file1.txt")).size, f1_001.size);

			Ls.File f1_002 = (com.kappaware.hsync.ttools.Ls.File) lsHdfs.getByPath("file1.txt_002");
			Assert.assertNotNull(f1_002);
			Assert.assertEquals(now / 1000, f1_002.modificationTime / 1000);
			Assert.assertEquals(((Ls.File) local.getByPath("b/file1.txt")).size, f1_002.size);
			
			Ls lsLocal = Ls.local(Utils.concatPath(lp, "c").toString());
			Assert.assertNotEquals(lsLocal, lsHdfs);		// Differs as there is _001 and _002

		}
	}

	@Test
	public void test08FileAdjust() throws IOException, ConfigurationException {
		String lp = (new File("src/test/resources/test08")).getAbsolutePath();
		Ls hdfs = Ls.hdfs("/tests/test08");
		Ls local = Ls.local(lp);
		hdfs.getFileSystem().copyFromLocalFile(Utils.concatPath(lp, "file1.txt"), new Path("/tests/test08/file1.txt"));
		// We must ensure we have same modification time to trigger only a right adjustment
		long now = (System.currentTimeMillis() / 1000) * 1000;
		local.getFileSystem().setTimes(Utils.concatPath(lp, "file1.txt"), (now), -1);
		hdfs.getFileSystem().setTimes(new Path("/tests/test08/file1.txt"), (now), -1);
		{
			Ls lsHdfs = Ls.hdfs("/tests/test08");
			Ls.File node = (Ls.File) lsHdfs.getByPath("file1.txt");
			Assert.assertNotNull(node);
			Assert.assertNotEquals("hdfs", node.owner);
			Assert.assertNotEquals("hadoop", node.group);
			Assert.assertNotEquals(0600, node.mode);

			Ls lsLocal = Ls.local(lp);
			Assert.assertEquals(lsLocal, lsHdfs);		
		}

		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test08", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600", "--reportFile", "./tmp/report08a.yml" };
		Main.main2(argv);
		File fr = new File("./tmp/report08a.yml");
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(0, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(1, report.getList("filesToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToDelete").size());
		{
			Ls lsHdfs = Ls.hdfs("/tests/test08");
			Ls.File node = (Ls.File) lsHdfs.getByPath("file1.txt");
			Assert.assertNotNull(node);
			Assert.assertEquals("hdfs", node.owner);
			Assert.assertEquals("hadoop", node.group);
			Assert.assertEquals(0600, node.mode);

			Ls lsLocal = Ls.local(lp);
			Assert.assertEquals(lsLocal, lsHdfs);	
			
		}
		this.checkNothingToDo(argv, "./tmp/report08b.yml");
	}

	
	@Test
	public void test09Excludes1() throws ConfigurationException, IOException {
		String lp = (new File("src/test/resources/test09_10")).getAbsolutePath();
		{
			String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test09", "--owner", "hdfs", "--group", "hadoop", // 
					"--fileMode", "0600", "--excludes", "**/.*", "--excludes", "**/tmp", "--reportFile", "./tmp/report09a.yml" };
			Main.main2(argv);

			File fr = new File("./tmp/report09a.yml");
			Report report = YamlUtils.parse(fr, Report.class);
			Assert.assertEquals(1, report.getList("foldersToCreate").size());
			Assert.assertEquals(0, report.getList("foldersToAdjust").size());
			Assert.assertEquals(2, report.getList("filesToCreate").size());
			Assert.assertEquals(0, report.getList("filesToReplace").size());
			Assert.assertEquals(0, report.getList("filesToAdjust").size());
			Assert.assertEquals(0, report.getList("filesToDelete").size());
			Assert.assertEquals(3, report.getList("excludedFolders").size());
			Assert.assertEquals(3, report.getList("excludedFiles").size());

			Ls ls = Ls.hdfs("/tests/test09");
			//System.out.println("============  " + ls.toString());
			Assert.assertEquals(3, ls.size());
			Assert.assertNotNull(ls.getByPath("file1.txt"));
			Assert.assertNotNull(ls.getByPath("subdir"));
			Assert.assertNotNull(ls.getByPath("subdir/file1.txt"));
			
			this.checkNothingToDo(argv, "./tmp/report09a.yml");
		}
		// And now, copy also the excluded
		{
			String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test09", "--owner", "hdfs", "--group", "hadoop", // 
					"--fileMode", "0600", "--reportFile", "./tmp/report09b.yml" };
			Main.main2(argv);

			File fr = new File("./tmp/report09b.yml");
			Report report = YamlUtils.parse(fr, Report.class);
			Assert.assertEquals(3, report.getList("foldersToCreate").size());
			Assert.assertEquals(0, report.getList("foldersToAdjust").size());
			Assert.assertEquals(6, report.getList("filesToCreate").size());
			Assert.assertEquals(0, report.getList("filesToReplace").size());
			Assert.assertEquals(0, report.getList("filesToAdjust").size());
			Assert.assertEquals(0, report.getList("filesToDelete").size());
			Assert.assertEquals(0, report.getList("excludedFolders").size());
			Assert.assertEquals(0, report.getList("excludedFiles").size());

			this.checkNothingToDo(argv, "./tmp/report09b.yml");

			Ls lsHdfs = Ls.hdfs("/tests/test09");
			//System.out.println("============ HDFS  " + lsHdfs.toString());
			Assert.assertEquals(12, lsHdfs.size());
			
			Ls lsLocal = Ls.local(lp);
			//System.out.println("============ LOCAL  " + lsLocal.toString());
			Assert.assertEquals(lsHdfs,  lsLocal);
		}
	}

	
	@Test
	public void test10Excludes2() throws ConfigurationException, IOException {
		String lp = (new File("src/test/resources/test09_10")).getAbsolutePath();
		{
			String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test10", "--owner", "hdfs", "--group", "hadoop", // 
					"--fileMode", "0600", "--excludes", "./.*", "--excludes", "./tmp", "--reportFile", "./tmp/report10a.yml" };
			Main.main2(argv);

			File fr = new File("./tmp/report10a.yml");
			Report report = YamlUtils.parse(fr, Report.class);
			Assert.assertEquals(2, report.getList("foldersToCreate").size());
			Assert.assertEquals(0, report.getList("foldersToAdjust").size());
			Assert.assertEquals(4, report.getList("filesToCreate").size());
			Assert.assertEquals(0, report.getList("filesToReplace").size());
			Assert.assertEquals(0, report.getList("filesToAdjust").size());
			Assert.assertEquals(0, report.getList("filesToDelete").size());
			Assert.assertEquals(2, report.getList("excludedFolders").size());
			Assert.assertEquals(2, report.getList("excludedFiles").size());

			Ls ls = Ls.hdfs("/tests/test10");
			//System.out.println("============  " + ls.toString());
			Assert.assertEquals(6, ls.size());
			Assert.assertNotNull(ls.getByPath("file1.txt"));
			Assert.assertNotNull(ls.getByPath("subdir"));
			Assert.assertNotNull(ls.getByPath("subdir/.profile"));
			Assert.assertNotNull(ls.getByPath("subdir/file1.txt"));
			Assert.assertNotNull(ls.getByPath("subdir/tmp"));
			Assert.assertNotNull(ls.getByPath("subdir/tmp/fileintmpinsubdir.txt"));
			
			this.checkNothingToDo(argv, "./tmp/report10b.yml");
		}
	}
	
	@Test
	public void test11MultiThread() throws ConfigurationException, IOException {
		String lp = (new File("src/test/resources")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test11", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600", "--folderMode", "0700", "--threads", "10"};
		Main.main2(argv);
		Ls lsHdfs = Ls.hdfs("/tests/test11");
		//System.out.println("============ HDFS  " + lsHdfs.toString());
		Ls lsLocal = Ls.local(lp);
		//System.out.println("============ LOCAL  " + lsLocal.toString());
		Assert.assertEquals(lsLocal, lsHdfs);
		this.checkNothingToDo(argv, "./tmp/report10.yml");
	}

	
	
	private void checkNothingToDo(String[] argv, String reportFile) throws JsonParseException, JsonMappingException, IOException, ConfigurationException {
		String[] argv2 = (String[]) ArrayUtils.addAll(argv, new String[] { "--reportFile", reportFile });
		Main.main2(argv2);
		File fr = new File(reportFile);
		Report report = YamlUtils.parse(fr, Report.class);
		Assert.assertEquals(0, report.getList("foldersToCreate").size());
		Assert.assertEquals(0, report.getList("foldersToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToCreate").size());
		Assert.assertEquals(0, report.getList("filesToReplace").size());
		Assert.assertEquals(0, report.getList("filesToAdjust").size());
		Assert.assertEquals(0, report.getList("filesToDelete").size());
	}

}
