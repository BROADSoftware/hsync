package com.kappaware.hsync;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kappaware.hsync.NotifierProtocol.COPY_STARTED;
import com.kappaware.hsync.NotifierProtocol.FILE_ADJUSTED;
import com.kappaware.hsync.NotifierProtocol.FILE_COPIED;
import com.kappaware.hsync.NotifierProtocol.FILE_DELETED;
import com.kappaware.hsync.NotifierProtocol.FILE_RENAMED;
import com.kappaware.hsync.NotifierProtocol.FOLDER_ADJUSTED;
import com.kappaware.hsync.NotifierProtocol.FOLDER_CREATED;
import com.kappaware.hsync.config.ConfigurationException;
import com.kappaware.hsync.ttools.Ls;
import com.kappaware.hsync.ttools.UnitAppender;
import com.kappaware.hsync.ttools.UnitHdfsCluster;
import com.kappaware.hsync.ttools.UnitKafka;
import com.kappaware.hsync.ttools.UnitKafka.KafkaResult;
import com.kappaware.hsync.ttools.UnitKafka.Position;

public class TestNotifier {

	static UnitHdfsCluster cluster;
	static UnitKafka<String, String> kafka;
	static ObjectMapper jsonMapper = new ObjectMapper();

	@BeforeClass
	public static void setup() throws Exception {
		cluster = new UnitHdfsCluster();
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
		// Apply a well defined set of permissions on source test tree, to have a well known start state
		String lp = (new File("src/test/resources/")).getAbsolutePath();
		Ls ls = Ls.local(lp);
		for (Ls.Node node : ls.getNodes()) {
			if (node instanceof Ls.File) {
				ls.getFileSystem().setPermission(Utils.concatPath(lp, node.getPath()), new FsPermission((short) 0644));
			} else if (node instanceof Ls.Folder) {
				ls.getFileSystem().setPermission(Utils.concatPath(lp, node.getPath()), new FsPermission((short) 0755));
			}
		}
		// And start the Kafka for notification
		kafka = new UnitKafka<String, String>(10000, 11111, String.class, String.class);
		kafka.startup();
		kafka.createTopic("test1", 1);

	}

	@AfterClass
	public static void tearDown() throws Exception {
		kafka.shutdown();
		cluster.stop();
	}

	public static class ClassUnderTest {
		private static final Logger log = Logger.getLogger(ClassUnderTest.class);

		public static void logMessage() {
			log.info("Hello Test (Info)");
			log.debug("Hello Test (Debug)");
		}
	}

	@Test
	public void test02Delete() throws IOException, ConfigurationException {

		UnitAppender testAppender = new UnitAppender();
		Logger.getRootLogger().addAppender(testAppender);
		Position position = kafka.getPosition();

		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(new Path("src/test/resources/test02/file1.txt"), new Path("/tests/test02/file0.txt._TMP_HSYNC_"));
		String lp = (new File("src/test/resources/test02b")).getAbsolutePath();
		new File(lp).mkdir();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test02", "--clientId", "testh", // 
				"--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
		};
		Main.main2(argv);

		//System.out.println("Logs:\n" + testAppender.toString());
		Assert.assertEquals(1, testAppender.searchPattern("FILE_DELETED").size());

		KafkaResult<String, String> result1 = kafka.fetch(position, 1);
		//System.out.print("************************************** result1:\n" + result1.toString());
		Assert.assertEquals(1, result1.size());
		FILE_DELETED fd = jsonMapper.readValue(result1.getRecords().get(0).value(), FILE_DELETED.class);
		Assert.assertEquals("testh", fd.clientId);
		Assert.assertEquals("FILE_DELETED", fd.action);
		Assert.assertEquals("/tests/test02/file0.txt._TMP_HSYNC_", fd.path);
	}

	@Test
	public void test03AdjustFolder() throws IOException, ConfigurationException {

		UnitAppender testAppender = new UnitAppender();
		Logger.getRootLogger().addAppender(testAppender);
		Position position = kafka.getPosition();

		FileSystem fs = FileSystem.get(new Configuration());
		fs.mkdirs(new Path("/tests/test03/"));
		fs.mkdirs(new Path("/tests/test03/folder1"));
		fs.mkdirs(new Path("/tests/test03/folder1/subfolder1"));
		//Ls ls = Ls.hdfs("/tests/test03");  System.out.println(ls.toString());
		String lp = (new File("src/test/resources/test03")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test03", "--owner", "hdfs", "--group", "hadoop", "--folderMode", "0700", //
				"--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
		};
		Main.main2(argv);

		//System.out.println("Logs:\n" + testAppender.toString());
		Assert.assertEquals(2, testAppender.searchPattern("FOLDER_ADJUSTED").size());
		Assert.assertEquals(1, testAppender.searchPattern("COPY_STARTED").size());
		Assert.assertEquals(1, testAppender.searchPattern("FILE_COPIED").size());

		KafkaResult<String, String> result1 = kafka.fetch(position, 1);
		//System.out.print("************************************** result1:\n" + result1.toString());
		Assert.assertEquals(4, result1.size());
		{
			FOLDER_ADJUSTED fa = jsonMapper.readValue(result1.getRecords().get(0).value(), FOLDER_ADJUSTED.class);
			Assert.assertEquals("hsync", fa.clientId);
			Assert.assertEquals("FOLDER_ADJUSTED", fa.action);
			Assert.assertEquals("/tests/test03/folder1/subfolder1", fa.path);
			Assert.assertEquals("hdfs", fa.owner);
			Assert.assertEquals("hadoop", fa.group);
			Assert.assertEquals(0700, fa.mode);
		}
		{
			FOLDER_ADJUSTED fa = jsonMapper.readValue(result1.getRecords().get(1).value(), FOLDER_ADJUSTED.class);
			Assert.assertEquals("hsync", fa.clientId);
			Assert.assertEquals("FOLDER_ADJUSTED", fa.action);
			Assert.assertEquals("/tests/test03/folder1", fa.path);
			Assert.assertEquals("hdfs", fa.owner);
			Assert.assertEquals("hadoop", fa.group);
			Assert.assertEquals(0700, fa.mode);
		}
		{
			COPY_STARTED cs = jsonMapper.readValue(result1.getRecords().get(2).value(), COPY_STARTED.class);
			Assert.assertEquals("hsync", cs.clientId);
			Assert.assertEquals("COPY_STARTED", cs.action);
			Assert.assertEquals("/tests/test03/folder1/subfolder1/file1.txt", cs.path);
		}
		{
			FILE_COPIED fc = jsonMapper.readValue(result1.getRecords().get(3).value(), FILE_COPIED.class);
			Assert.assertEquals("hsync", fc.clientId);
			Assert.assertEquals("FILE_COPIED", fc.action);
			Assert.assertEquals("/tests/test03/folder1/subfolder1/file1.txt", fc.path);
			Assert.assertEquals("hdfs", fc.owner);
			Assert.assertEquals("hadoop", fc.group);
			Assert.assertEquals(0644, fc.mode);
			Assert.assertEquals(41, fc.size);
		}

	}

	@Test
	public void test04CreateFolder() throws IOException, ConfigurationException {

		UnitAppender testAppender = new UnitAppender();
		Logger.getRootLogger().addAppender(testAppender);
		Position position = kafka.getPosition();

		String lp = (new File("src/test/resources/test04")).getAbsolutePath();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test04", "--owner", "hdfs", "--group", "hadoop", "--folderMode", "0700", //
				"--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
		};
		Main.main2(argv);

		//System.out.println("Logs:\n" + testAppender.toString());
		Assert.assertEquals(2, testAppender.searchPattern("FOLDER_CREATED").size());
		Assert.assertEquals(1, testAppender.searchPattern("COPY_STARTED").size());
		Assert.assertEquals(1, testAppender.searchPattern("FILE_COPIED").size());

		KafkaResult<String, String> result1 = kafka.fetch(position, 1);
		//System.out.print("************************************** result1:\n" + result1.toString());
		Assert.assertEquals(4, result1.size());
		{
			FOLDER_CREATED fa = jsonMapper.readValue(result1.getRecords().get(0).value(), FOLDER_CREATED.class);
			Assert.assertEquals("hsync", fa.clientId);
			Assert.assertEquals("FOLDER_CREATED", fa.action);
			Assert.assertEquals("/tests/test04/folder1", fa.path);
			Assert.assertEquals("hdfs", fa.owner);
			Assert.assertEquals("hadoop", fa.group);
			Assert.assertEquals(0700, fa.mode);
		}
		{
			FOLDER_CREATED fa = jsonMapper.readValue(result1.getRecords().get(1).value(), FOLDER_CREATED.class);
			Assert.assertEquals("hsync", fa.clientId);
			Assert.assertEquals("FOLDER_CREATED", fa.action);
			Assert.assertEquals("/tests/test04/folder1/subfolder1", fa.path);
			Assert.assertEquals("hdfs", fa.owner);
			Assert.assertEquals("hadoop", fa.group);
			Assert.assertEquals(0700, fa.mode);
		}
		{
			COPY_STARTED cs = jsonMapper.readValue(result1.getRecords().get(2).value(), COPY_STARTED.class);
			Assert.assertEquals("hsync", cs.clientId);
			Assert.assertEquals("COPY_STARTED", cs.action);
			Assert.assertEquals("/tests/test04/folder1/subfolder1/file1.txt", cs.path);
		}
		{
			FILE_COPIED fc = jsonMapper.readValue(result1.getRecords().get(3).value(), FILE_COPIED.class);
			Assert.assertEquals("hsync", fc.clientId);
			Assert.assertEquals("FILE_COPIED", fc.action);
			Assert.assertEquals("/tests/test04/folder1/subfolder1/file1.txt", fc.path);
			Assert.assertEquals("hdfs", fc.owner);
			Assert.assertEquals("hadoop", fc.group);
			Assert.assertEquals(0644, fc.mode);
			Assert.assertEquals(41, fc.size);
		}
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
			UnitAppender testAppender = new UnitAppender();
			Logger.getRootLogger().addAppender(testAppender);
			Position position = kafka.getPosition();

			String[] argv1 = new String[] { "--localPath", Utils.concatPath(lp, "a").toString(), "--hdfsPath", "/tests/test07", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600", //
					"--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
			};
			Main.main2(argv1);
			//System.out.println("Logs:\n" + testAppender.toString());
			Assert.assertEquals(1, testAppender.searchPattern("COPY_STARTED").size());
			Assert.assertEquals(1, testAppender.searchPattern("FILE_COPIED").size());

			KafkaResult<String, String> result1 = kafka.fetch(position, 2);
			//System.out.print("************************************** result1:\n" + result1.toString());
			Assert.assertEquals(2, result1.size());
			{
				COPY_STARTED cs = jsonMapper.readValue(result1.getRecords().get(0).value(), COPY_STARTED.class);
				Assert.assertEquals("hsync", cs.clientId);
				Assert.assertEquals("COPY_STARTED", cs.action);
				Assert.assertEquals("/tests/test07/file1.txt", cs.path);
			}
			{
				FILE_COPIED fc = jsonMapper.readValue(result1.getRecords().get(1).value(), FILE_COPIED.class);
				Assert.assertEquals("hsync", fc.clientId);
				Assert.assertEquals("FILE_COPIED", fc.action);
				Assert.assertEquals("/tests/test07/file1.txt", fc.path);
				Assert.assertEquals("hdfs", fc.owner);
				Assert.assertEquals("hadoop", fc.group);
				Assert.assertEquals(0600, fc.mode);
				Assert.assertEquals(41, fc.size);
				Assert.assertEquals((now - 3600000) / 1000, fc.modificationTime / 1000);
			}

		}
		{
			UnitAppender testAppender = new UnitAppender();
			Logger.getRootLogger().addAppender(testAppender);
			Position position = kafka.getPosition();

			String[] argv2 = new String[] { "--localPath", Utils.concatPath(lp, "b").toString(), "--hdfsPath", "/tests/test07", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600", //
					"--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
			};
			Main.main2(argv2);
			//System.out.println("Logs:\n" + testAppender.toString());
			Assert.assertEquals(1, testAppender.searchPattern("FILE_RENAMED").size());
			Assert.assertEquals(1, testAppender.searchPattern("COPY_STARTED").size());
			Assert.assertEquals(1, testAppender.searchPattern("FILE_COPIED").size());

			KafkaResult<String, String> result1 = kafka.fetch(position, 3);
			//System.out.print("************************************** result1:\n" + result1.toString());
			Assert.assertEquals(3, result1.size());
			{
				FILE_RENAMED cs = jsonMapper.readValue(result1.getRecords().get(0).value(), FILE_RENAMED.class);
				Assert.assertEquals("hsync", cs.clientId);
				Assert.assertEquals("FILE_RENAMED", cs.action);
				Assert.assertEquals("/tests/test07/file1.txt", cs.oldName);
				Assert.assertEquals("/tests/test07/file1.txt_001", cs.newName);
			}
			{
				COPY_STARTED cs = jsonMapper.readValue(result1.getRecords().get(1).value(), COPY_STARTED.class);
				Assert.assertEquals("hsync", cs.clientId);
				Assert.assertEquals("COPY_STARTED", cs.action);
				Assert.assertEquals("/tests/test07/file1.txt", cs.path);
			}
			{
				FILE_COPIED fc = jsonMapper.readValue(result1.getRecords().get(2).value(), FILE_COPIED.class);
				Assert.assertEquals("hsync", fc.clientId);
				Assert.assertEquals("FILE_COPIED", fc.action);
				Assert.assertEquals("/tests/test07/file1.txt", fc.path);
				Assert.assertEquals("hdfs", fc.owner);
				Assert.assertEquals("hadoop", fc.group);
				Assert.assertEquals(0600, fc.mode);
				Assert.assertEquals(41, fc.size);
				Assert.assertEquals((now) / 1000, fc.modificationTime / 1000);
			}

		}
		{
			UnitAppender testAppender = new UnitAppender();
			Logger.getRootLogger().addAppender(testAppender);
			Position position = kafka.getPosition();

			String[] argv3 = new String[] { "--localPath", Utils.concatPath(lp, "c").toString(), "--hdfsPath", "/tests/test07", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600", "--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
			};
			Main.main2(argv3);

			//System.out.println("Logs:\n" + testAppender.toString());
			Assert.assertEquals(1, testAppender.searchPattern("FILE_RENAMED").size());
			Assert.assertEquals(1, testAppender.searchPattern("COPY_STARTED").size());
			Assert.assertEquals(1, testAppender.searchPattern("FILE_COPIED").size());
			KafkaResult<String, String> result1 = kafka.fetch(position, 3);
			//System.out.print("************************************** result1:\n" + result1.toString());
			Assert.assertEquals(3, result1.size());
			{
				FILE_RENAMED cs = jsonMapper.readValue(result1.getRecords().get(0).value(), FILE_RENAMED.class);
				Assert.assertEquals("hsync", cs.clientId);
				Assert.assertEquals("FILE_RENAMED", cs.action);
				Assert.assertEquals("/tests/test07/file1.txt", cs.oldName);
				Assert.assertEquals("/tests/test07/file1.txt_002", cs.newName);
			}
			{
				COPY_STARTED cs = jsonMapper.readValue(result1.getRecords().get(1).value(), COPY_STARTED.class);
				Assert.assertEquals("hsync", cs.clientId);
				Assert.assertEquals("COPY_STARTED", cs.action);
				Assert.assertEquals("/tests/test07/file1.txt", cs.path);
			}
			{
				FILE_COPIED fc = jsonMapper.readValue(result1.getRecords().get(2).value(), FILE_COPIED.class);
				Assert.assertEquals("hsync", fc.clientId);
				Assert.assertEquals("FILE_COPIED", fc.action);
				Assert.assertEquals("/tests/test07/file1.txt", fc.path);
				Assert.assertEquals("hdfs", fc.owner);
				Assert.assertEquals("hadoop", fc.group);
				Assert.assertEquals(0600, fc.mode);
				Assert.assertEquals(56, fc.size);
				Assert.assertEquals((now) / 1000, fc.modificationTime / 1000);
			}
		}
	}

	@Test
	public void test08FileAdjust() throws IOException, ConfigurationException {

		UnitAppender testAppender = new UnitAppender();
		Logger.getRootLogger().addAppender(testAppender);
		Position position = kafka.getPosition();

		String lp = (new File("src/test/resources/test08")).getAbsolutePath();
		Ls hdfs = Ls.hdfs("/tests/test08");
		Ls local = Ls.local(lp);
		hdfs.getFileSystem().copyFromLocalFile(Utils.concatPath(lp, "file1.txt"), new Path("/tests/test08/file1.txt"));
		// We must ensure we have same modification time to trigger only a right adjustment
		long now = (System.currentTimeMillis() / 1000) * 1000;
		local.getFileSystem().setTimes(Utils.concatPath(lp, "file1.txt"), (now), -1);
		hdfs.getFileSystem().setTimes(new Path("/tests/test08/file1.txt"), (now), -1);

		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test08", "--owner", "hdfs", "--group", "hadoop", "--fileMode", "0600", "--notifier", "logs://info", "--notifier", "kafka://" + kafka.getBrokerString() + "/test1" // 
		};
		Main.main2(argv);

		//System.out.println("Logs:\n" + testAppender.toString());
		Assert.assertEquals(1, testAppender.searchPattern("FILE_ADJUSTED").size());

		KafkaResult<String, String> result1 = kafka.fetch(position, 1);
		//System.out.print("************************************** result1:\n" + result1.toString());
		Assert.assertEquals(1, result1.size());
		{
			FILE_ADJUSTED fc = jsonMapper.readValue(result1.getRecords().get(0).value(), FILE_ADJUSTED.class);
			Assert.assertEquals("hsync", fc.clientId);
			Assert.assertEquals("FILE_ADJUSTED", fc.action);
			Assert.assertEquals("/tests/test08/file1.txt", fc.path);
			Assert.assertEquals("hdfs", fc.owner);
			Assert.assertEquals("hadoop", fc.group);
			Assert.assertEquals(0600, fc.mode);
		}

	}

}
