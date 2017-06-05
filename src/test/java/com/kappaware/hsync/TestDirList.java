package com.kappaware.hsync;

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
	public void test00() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(new Path("src/test/resources/log4j.xml"), new Path("/tests/test00/log4j.xml"));

		DirList dirList = new DirList("/");
		System.out.println(dirList.toString());
		Assert.assertNotNull(dirList.getByPath("/tests/test00"));
		Assert.assertNotNull(dirList.getByPath("/tests/test00"));
		Assert.assertNotNull(dirList.getByPath("/tests/test00/log4j.xml"));
		
		DirList dirList2 = new DirList("/tests");
		//System.out.println(dirList2.toString());
		Assert.assertNotNull(dirList2.getByPath("test00"));
		Assert.assertNotNull(dirList2.getByPath("test00/log4j.xml"));

		DirList dirList3 = new DirList("/tests/test00");
		//System.out.println(dirList3.toString());
		Assert.assertNotNull(dirList3.getByPath("log4j.xml"));
	}


}
