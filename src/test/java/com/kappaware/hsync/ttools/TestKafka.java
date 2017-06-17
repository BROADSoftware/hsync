package com.kappaware.hsync.ttools;


import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.kappaware.hsync.ttools.UnitKafka.KafkaResult;
import com.kappaware.hsync.ttools.UnitKafka.Position;


public class TestKafka {
	static UnitKafka<String, String> kafka;

	@BeforeClass
	public static void setup() throws Exception {
		kafka = new UnitKafka<String, String>(10000, 11111, String.class, String.class);
		kafka.startup();
		kafka.createTopic("test1", 1);
		kafka.createTopic("test2", 1);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		kafka.shutdown();
	}
	
	
	@Test
	public void test1() {
		//MiniKafka.sleep(2000);
		Position pos1 = kafka.getPosition();
		kafka.sendMessage("test1", "key11", "value11");
		kafka.sendMessage("test1", "key12", "value12");
		UnitKafka.sleep(2000);
		Position pos2 = kafka.getPosition();
		kafka.sendMessage("test2", "key21", "value21");
		kafka.sendMessage("test1", "key13", "value13");
		kafka.sendMessage("test2", "key22", "value22");

		//System.out.println("----------------------- Position1:\n" + pos1.toString());
		//System.out.println("----------------------- Position2:\n" + pos2.toString());

		KafkaResult<String, String> result1 = kafka.fetch(pos1, 5);
		//System.out.print("************************************** result1:\n" + result1.toString());
		Assert.assertEquals("key11", result1.getRecord("test1", 0).key());
		Assert.assertEquals("key12", result1.getRecord("test1", 1).key());
		Assert.assertEquals("key13", result1.getRecord("test1", 2).key());
		Assert.assertEquals("key21", result1.getRecord("test2", 0).key());
		Assert.assertEquals("key22", result1.getRecord("test2", 1).key());

		KafkaResult<String, String> result2 = kafka.fetch(pos2, 3);
		//System.out.print("************************************** result2:\n" + result2.toString());
		Assert.assertEquals(3,  result2.size());
		Assert.assertEquals("key13", result2.getRecord("test1", 0).key());
		Assert.assertEquals("key21", result2.getRecord("test2", 0).key());
		Assert.assertEquals("key22", result2.getRecord("test2", 1).key());
		

	}

	// Almost same test, be ensure test independancy
	@Test
	public void test2() {
		//MiniKafka.sleep(2000);
		Position pos1 = kafka.getPosition();
		kafka.sendMessage("test1", "key11", "value11");
		kafka.sendMessage("test1", "key12", "value12");
		UnitKafka.sleep(2000);
		Position pos2 = kafka.getPosition();
		kafka.sendMessage("test2", "key21", "value21");
		kafka.sendMessage("test1", "key13", "value13");
		kafka.sendMessage("test2", "key22", "value22");

		//System.out.println("----------------------- Position1:\n" + pos1.toString());
		//System.out.println("----------------------- Position2:\n" + pos2.toString());
		
		KafkaResult<String, String> result2 = kafka.fetch(pos2, 3);
		//System.out.print("************************************** result2:\n" + result2.toString());
		Assert.assertEquals(3,  result2.size());
		Assert.assertEquals("key13", result2.getRecord("test1", 0).key());
		Assert.assertEquals("key21", result2.getRecord("test2", 0).key());
		Assert.assertEquals("key22", result2.getRecord("test2", 1).key());
		

		KafkaResult<String, String> result1 = kafka.fetch(pos1, 5);
		//System.out.print("************************************** result1:\n" + result1.toString());
		Assert.assertEquals("key11", result1.getRecord("test1", 0).key());
		Assert.assertEquals("key12", result1.getRecord("test1", 1).key());
		Assert.assertEquals("key13", result1.getRecord("test1", 2).key());
		Assert.assertEquals("key21", result1.getRecord("test2", 0).key());
		Assert.assertEquals("key22", result1.getRecord("test2", 1).key());
	}

}
