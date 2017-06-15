package com.kappaware.hsync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kappaware.hsync.config.ConfigurationException;
import com.kappaware.hsync.ttools.Ls;
import com.kappaware.hsync.ttools.MiniHdfsCluster;

public class TestNotifier {

	public static class TestAppender extends AppenderSkeleton {
		public List<LoggingEvent> events = new ArrayList<LoggingEvent>();

		public void close() {
		}

		public boolean requiresLayout() {
			return false;
		}

		@Override
		protected void append(LoggingEvent event) {
			events.add(event);
		}
		
		void print() {
			for(LoggingEvent event : this.events) {
				System.out.println("------------->" + event.getLevel().toString() + ": " + event.getMessage().toString());
			}
		}
	}

	
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
		// Apply a well defined set of permissions on source test tree, to have a well known start state
		String lp = (new File("src/test/resources/")).getAbsolutePath();
		Ls ls = Ls.local(lp);
		for(Ls.Node node : ls.getNodes()) {
			if(node instanceof Ls.File) {
				ls.getFileSystem().setPermission(Utils.concatPath(lp, node.getPath()), new FsPermission((short)0644));
			} else if(node instanceof Ls.Folder) {
				ls.getFileSystem().setPermission(Utils.concatPath(lp, node.getPath()), new FsPermission((short)0755));
			}
		}
	}

	@AfterClass
	public static void tearDown() throws Exception {
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
	public void test1() {
	    TestAppender testAppender = new TestAppender();
        Logger.getRootLogger().addAppender(testAppender);

        ClassUnderTest.logMessage();
        
        testAppender.print();
	}

	@Test
	public void test2() throws IOException, ConfigurationException {
	    TestAppender testAppender = new TestAppender();
        Logger.getRootLogger().addAppender(testAppender);

		FileSystem fs = FileSystem.get(new Configuration());
		fs.copyFromLocalFile(new Path("src/test/resources/test02/file1.txt"), new Path("/tests/test02/file0.txt.tmp_hsync"));
		//System.out.println(Ls.hdfs("/tests").toString());
		// Need to create an empty folder
		String lp = (new File("src/test/resources/test02b")).getAbsolutePath();
		new File(lp).mkdir();
		String[] argv = new String[] { "--localPath", lp, "--hdfsPath", "/tests/test02", "--reportFile", "./tmp/report02.yml", "--notifier", "logs://info", "--notifier", "logs://debug", "--clientId", "testh"};
		Main.main2(argv);
        
        testAppender.print();
	}
	
	
}
