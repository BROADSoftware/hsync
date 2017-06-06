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


public class TestDirList {
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

	/**
	 * For testing of DirList.
	 * @throws IOException
	 */
	@Test
	public void testDirList00Hdfs() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(new Path("src/test/resources/log4j.xml"), new Path("/tests/test00/log4j.xml"));

		Ls dirList = Ls.hdfs("/");
		System.out.println(dirList.toString());
		Assert.assertNotNull(dirList.getByPath("/tests/test00"));
		Assert.assertNotNull(dirList.getByPath("/tests/test00"));
		Assert.assertNotNull(dirList.getByPath("/tests/test00/log4j.xml"));
		
		Ls dirList2 = Ls.hdfs("/tests");
		//System.out.println(dirList2.toString());
		Assert.assertNotNull(dirList2.getByPath("test00"));
		Assert.assertNotNull(dirList2.getByPath("test00/log4j.xml"));

		Ls dirList3 = Ls.hdfs("/tests/test00");
		//System.out.println(dirList3.toString());
		Assert.assertNotNull(dirList3.getByPath("log4j.xml"));
	}

	@Test
	public void testDirList01Local() throws IOException {
		String lp = (new File("src/test/resources/test01")).getAbsolutePath();
		Ls dirList = Ls.local(lp);
		System.out.println(dirList.toString());
		Assert.assertNotNull(dirList.getByPath("file0"));
		Assert.assertNotNull(dirList.getByPath("folder1"));
		Assert.assertNotNull(dirList.getByPath("folder1/subfolder1/subfile1"));
	}



}
