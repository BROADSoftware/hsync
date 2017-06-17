/*
 * Copyright (C) 2014 Christopher Batey
 * Copyright (C) 2017 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kappaware.hsync.ttools;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.TopicCommand;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZkUtils;



/**
 * This class aim to provide unit testing tooling for modules using Kafka.
 * 
 * Beside offering a Kafka cluster, there is also a producer and consumers.
 * 
 * Consumer is specific as it is assigned to all partitions it can access. 
 * NB: As topic creation can take some time to be visible, there is a loop to wait for all created partition to be visible. 
 * This test can be defeated if a topic is created by another way than the provided createTopic().
 * 
 * Also, there is a getPosition() feature. Intend is to be able to make each test independent without rebuilding a new cluster on each one.
 * getPosition() will allow take a picture of current offsets, perform tested operation and the fetch from this position to get only newly created events.
 * 
 * Unfortunately there is some delay between write and read, so we sometime need to insert some delays. 
 * 
 * @author sa
 *
 */
public class UnitKafka<K, V> {

	private static final Logger log = LoggerFactory.getLogger(UnitKafka.class);

	private KafkaServerStartable broker;

	private UnitZookeeper zookeeper;
	private String zookeeperString;
	private String brokerString;
	private Serde<K> keySerde;
	private Serde<V> valueSerde;
	private int zkPort;
	private int brokerPort;
	private Properties kafkaBrokerConfig = new Properties();
	private KafkaProducer<K, V> producer = null;
	private KafkaConsumer<K, V> _consumer = null;
	
	private int partitionCount = 0;
	private List<TopicPartition> topicPartitions;

	public UnitKafka(Class<K> keyClass, Class<V> valueClass) throws IOException {
		this(getEphemeralPort(), getEphemeralPort(), keyClass, valueClass);
	}

	public UnitKafka(int zkPort, int brokerPort, Class<K> keyClass, Class<V> valueClass) {
		this.zkPort = zkPort;
		this.brokerPort = brokerPort;
		this.zookeeperString = "localhost:" + zkPort;
		this.brokerString = "localhost:" + brokerPort;
		this.keySerde = (Serde<K>) Serdes.serdeFrom(keyClass);
		this.valueSerde = (Serde<V>) Serdes.serdeFrom(valueClass);
	}

	private static int getEphemeralPort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	public void startup() {
		zookeeper = new UnitZookeeper(zkPort);
		zookeeper.startup();

		final File logDir;
		try {
			logDir = Files.createTempDirectory("kafka").toFile();
		} catch (IOException e) {
			throw new RuntimeException("Unable to start Kafka", e);
		}
		logDir.deleteOnExit();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					FileUtils.deleteDirectory(logDir);
				} catch (IOException e) {
					log.warn("Problems deleting temporary directory " + logDir.getAbsolutePath(), e);
				}
			}
		}));
		kafkaBrokerConfig.setProperty("zookeeper.connect", zookeeperString);
		kafkaBrokerConfig.setProperty("broker.id", "1");
		kafkaBrokerConfig.setProperty("host.name", "localhost");
		kafkaBrokerConfig.setProperty("port", Integer.toString(brokerPort));
		kafkaBrokerConfig.setProperty("log.dir", logDir.getAbsolutePath());
		kafkaBrokerConfig.setProperty("log.flush.interval.messages", String.valueOf(1));

		broker = new KafkaServerStartable(new KafkaConfig(kafkaBrokerConfig));
		broker.startup();
	}

	public void shutdown() {
		if (broker != null)
			broker.shutdown();
		if (zookeeper != null)
			zookeeper.shutdown();
	}

	public void createTopic(String topicName, Integer numPartitions) {
		// setup
		String[] arguments = new String[9];
		arguments[0] = "--create";
		arguments[1] = "--zookeeper";
		arguments[2] = zookeeperString;
		arguments[3] = "--replication-factor";
		arguments[4] = "1";
		arguments[5] = "--partitions";
		arguments[6] = "" + Integer.valueOf(numPartitions);
		arguments[7] = "--topic";
		arguments[8] = topicName;
		TopicCommand.TopicCommandOptions opts = new TopicCommand.TopicCommandOptions(arguments);

		ZkUtils zkUtils = ZkUtils.apply(opts.options().valueOf(opts.zkConnectOpt()), 30000, 30000, JaasUtils.isZkSecurityEnabled());
		// run
		log.info("Executing: CreateTopic " + Arrays.toString(arguments));
		TopicCommand.createTopic(zkUtils, opts);
		this.partitionCount += numPartitions;
		this.resetConsumer();
	}

	public void sendMessage(ProducerRecord<K, V> record) {
		if (this.producer == null) {
			Properties props = new Properties();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerString);
			props.put(ProducerConfig.ACKS_CONFIG, "all");
			// If topic is not present, fail in 2 secs
			props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
			this.producer = new KafkaProducer<K, V>(props, this.keySerde.serializer(), this.valueSerde.serializer());
		}
		this.producer.send(record);
	}

	public void sendMessage(String topic, K key, V value) {
		ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, key, value);
		this.sendMessage(record);
	}

	synchronized private KafkaConsumer<K, V> getConsumer() {
		if (this._consumer == null) {
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.brokerString);
			props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kdescribe");
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
			this._consumer = new KafkaConsumer<K, V>(props, this.keySerde.deserializer(), this.valueSerde.deserializer());
			// assign ALL topics. We loop until we get at least all topics/partition we have created.
			long maxTime = System.currentTimeMillis() + 5000;
			do {
				sleep(100);
				this.topicPartitions = new Vector<TopicPartition>();
				for (List<PartitionInfo> pinfos : this._consumer.listTopics().values()) {
					for (PartitionInfo pinfo : pinfos) {
						this.topicPartitions.add(new TopicPartition(pinfo.topic(), pinfo.partition()));
					}
				}
				if(System.currentTimeMillis() > maxTime) {
					throw new RuntimeException(String.format("Time limit exceeded for assigning %d partitions", this.partitionCount));
				}
			} while (this.topicPartitions.size() < this.partitionCount);
			this._consumer.assign(this.topicPartitions);
		}
		return this._consumer;
	}

	synchronized public void resetConsumer() {
		this._consumer = null;
	}
	

	
	// This class just to a nicer type than  Map<TopicPartition, Long>
	static public class Position {
		private Map<TopicPartition, Long> value;

		public Position(Map<TopicPartition, Long> value) {
			this.value = value;
		}

		public Map<TopicPartition, Long> getValue() {
			return value;
		}
		
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			for(Map.Entry<TopicPartition, Long> entry : this.value.entrySet()) {
				sb.append(String.format("Topic '%s'  part# %d   offset:%d\n", entry.getKey().topic(), entry.getKey().partition(), entry.getValue()));
			}
			return sb.toString();
		}
	}

	public Position getPosition() {
		return new Position(this.getConsumer().endOffsets(this.topicPartitions));
	}

	public KafkaResult<K, V> fetch(Position position, int expected) {
		for (Map.Entry<TopicPartition, Long> entry : position.getValue().entrySet()) {
			log.debug(String.format("Set topic '%s' part# %d   to offset %d", entry.getKey().topic(), entry.getKey().partition(), entry.getValue()));
			this.getConsumer().seek(entry.getKey(), entry.getValue());
		}
		KafkaResult<K, V> result = new KafkaResult<K, V>();
		ConsumerRecords<K, V> records;
		long maxTime = System.currentTimeMillis() + 5000;
		do {
			records = getConsumer().poll(100);
			for (ConsumerRecord<K, V> record : records) {
				result.add(record);
			}
			if(System.currentTimeMillis() > maxTime) {
				throw new RuntimeException(String.format("Time limit exceeded for fetching %d records", expected));
			}
		} while (result.size() < expected);
		return result;
	}

	static public class KafkaResult<K, V> {
		private Map<String, List<ConsumerRecord<K, V>>> recordByTopic = new HashMap<String, List<ConsumerRecord<K, V>>>();
		private List<ConsumerRecord<K,V>> records = new Vector<ConsumerRecord<K,V>>();

		void add(ConsumerRecord<K, V> record) {
			this.records.add(record);
			List<ConsumerRecord<K, V>> l = this.recordByTopic.get(record.topic());
			if (l == null) {
				l = new Vector<ConsumerRecord<K, V>>();
				this.recordByTopic.put(record.topic(), l);
			}
			l.add(record);
		}

		ConsumerRecord<K, V> getRecord(String topic, int position) {
			if (recordByTopic.containsKey(topic) && recordByTopic.get(topic).size() > position) {
				return recordByTopic.get(topic).get(position);
			} else {
				return null;
			}
		}

		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			for (Map.Entry<String, List<ConsumerRecord<K, V>>> entry : this.recordByTopic.entrySet()) {
				sb.append(String.format("Topic: '%s':\n", entry.getKey()));
				for (ConsumerRecord<K, V> record : entry.getValue()) {
					sb.append(String.format("\tkey: '%s'   value:'%s'\n", record.key(), record.value()));
				}
			}
			return sb.toString();
		}
		
		public List<ConsumerRecord<K,V>> getRecords() {
			return this.records;
		}

		public int size() {
			return this.records.size();
		}
	}
	
	public String getBrokerString() {
		return brokerString;
	}

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
