package com.kappaware.hsync.ttools;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kappaware.hsync.ttools.UnitAppender;

public class TestAppender {


	

	@BeforeClass
	public static void setup() throws Exception {
	}

	@AfterClass
	public static void tearDown() throws Exception {
	}

	
	
	public static class ClassUnderTest {
		private static final Logger log = Logger.getLogger(ClassUnderTest.class);
		static {
			log.setLevel(Level.DEBUG);
		}
		public static void logMessage() {
			log.debug("Hello Test (Debug)");
			log.info("Hello Test (Info)");
			log.warn("Hello Test (Warn)");
			log.error("Hello Test (Error)");
		}
	}

	@Test
	public void test1() {
	    UnitAppender testAppender = new UnitAppender();
        Logger.getRootLogger().addAppender(testAppender);

        ClassUnderTest.logMessage();
        System.out.println("Logs:\n" + testAppender.toString());
        Assert.assertEquals(4, testAppender.getEvents().size());
        Assert.assertEquals(Level.DEBUG, testAppender.getEvents().get(0).getLevel());
        Assert.assertEquals("Hello Test (Debug)", testAppender.getEvents().get(0).getMessage());
        Assert.assertEquals(Level.INFO, testAppender.getEvents().get(1).getLevel());
        Assert.assertEquals(Level.WARN, testAppender.getEvents().get(2).getLevel());
        Assert.assertEquals(Level.ERROR, testAppender.getEvents().get(3).getLevel());
        Assert.assertEquals(4,  testAppender.searchPattern("Hello").size());
        Assert.assertEquals(1,  testAppender.searchPattern("Error").size());

	
	}
	
}
