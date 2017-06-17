package com.kappaware.hsync.notifier;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.kappaware.hsync.config.ConfigurationException;

public class KafkaNotifier extends JsonFormater implements Notifier {
	private KafkaProducer<String, String> producer;
	private String topic;
	
	// --------------------------------------------------------------------------------------------------- Kafka stuff
	
	public KafkaNotifier(String clientId, String desc) throws ConfigurationException {
		super(clientId);
		String withoutQuery;
		String query;
		String format;
		Properties properties;
		//-------------------------- Split query part
		int p = desc.indexOf('?');
		if (p == -1) {
			withoutQuery = desc;
			query = null;
		} else {
			withoutQuery = desc.substring(0, p);
			query = desc.substring(p + 1);
		}

		if (!withoutQuery.startsWith("kafka://")) {
			// Should never occurs, as already ensure by factory
			throw new ConfigurationException(String.format("Unreconized notification sink '%s'", withoutQuery));
		}
		String x = withoutQuery.substring("kafka://".length());
		String[] parts = x.split("/");
		if (parts.length != 2) {
			throw new ConfigurationException(String.format("'%s' is not a valid kafka target. Must be of the the form:  kafka://broker1:9092,broker2:9092,.../topic", withoutQuery));
		}
		String brokers = parts[0];
		this.topic = parts[1];
		Map<String, String> params = handleParams(query);
		if (params.containsKey("format")) {
			format = params.get("format").toLowerCase();
			params.remove("format");
		} else {
			format = "json";
		}
		if(!"json".equals(format)) {
			throw new ConfigurationException(String.format("Sorry, currently only 'JSON' format is supported on Kafka notifier."));
		}
		properties = buildProperties(params, brokers, clientId, false);
		
		this.producer = new KafkaProducer<String, String>(properties, new StringSerializer(), new StringSerializer());
		// Test target topic (We want to fail immediately in init in this case)
		try {
			producer.partitionsFor(this.topic).size();
		} catch (Exception e) {
			producer.close();
			throw new ConfigurationException(String.format("Unable to access topic '%s': %s.", this.topic, e.toString()));
		}
	}

	// May be used by other notifiers in future
	private static Map<String, String> handleParams(String query) throws ConfigurationException {
		try {
			Map<String, String> params = new HashMap<String, String>();
			if (query != null && query.trim().length() > 0) {
				String[] pairs = query.split("&");
				for (String pair : pairs) {
					int idx = pair.indexOf("=");
					if (idx != -1) {
						params.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));
					} else {
						params.put(URLDecoder.decode(pair, "UTF-8"), "true");
					}
				}
			}
			return params;
		} catch (UnsupportedEncodingException e) {
			throw new ConfigurationException(e.getMessage());
		}
	}

	// @formatter:off
	@SuppressWarnings("deprecation")
	static Set<String> validProducerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ProducerConfig.MAX_BLOCK_MS_CONFIG, 
		ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, 
		ProducerConfig.METADATA_MAX_AGE_CONFIG, 
		ProducerConfig.BATCH_SIZE_CONFIG, 
		ProducerConfig.BUFFER_MEMORY_CONFIG, 
		ProducerConfig.ACKS_CONFIG, 
		ProducerConfig.TIMEOUT_CONFIG, 
		ProducerConfig.LINGER_MS_CONFIG, 
		ProducerConfig.SEND_BUFFER_CONFIG, 
		ProducerConfig.RECEIVE_BUFFER_CONFIG, 
		ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 
		ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 
		ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, 
		ProducerConfig.RETRIES_CONFIG, 
		ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 
		ProducerConfig.COMPRESSION_TYPE_CONFIG, 
		ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 
		ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, 
		ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, 
		ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 
		ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 
		ProducerConfig.PARTITIONER_CLASS_CONFIG, 
		ProducerConfig.MAX_BLOCK_MS_CONFIG, 
		ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 
		ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
		ProducerConfig.CLIENT_ID_CONFIG, 
		CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
	}));

	static Set<String> protectedProducerProperties = new HashSet<String>(Arrays.asList(new String[] { 
		ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
		ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
		ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
	}));
	// @formatter:on

	public static Properties buildProperties(Map<String, String> params, String brokers, String clientId, boolean forceProperties) throws ConfigurationException {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

		for (Map.Entry<String, String> entry : params.entrySet()) {
			String propName = entry.getKey();
			if (forceProperties) {
				properties.put(propName, entry.getValue());
			} else {
				if (validProducerProperties.contains(propName)) {
					properties.put(propName, entry.getValue());
				} else if (protectedProducerProperties.contains(propName)) {
					throw new ConfigurationException(String.format("Usage of target property '%s' is reserved by this application!", propName));
				} else {
					throw new ConfigurationException(String.format("Invalid target property '%s'!", propName));
				}
			}
		}
		// If topic is not present, fail in 2 secs
		if (!properties.containsKey(ProducerConfig.MAX_BLOCK_MS_CONFIG)) {
			properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
		}
		return properties;
	}

	
	private void send(String message) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, null, message);
		this.producer.send(record);
	}
	
	// ----------------------------------------------------------------------------------------- Notifier stuff
	
	@Override
	public void fileRenamed(String oldName, String newName) {
		this.send(this._fileRenamed(oldName, newName));
	}

	@Override
	public void folderCreated(String path, String owner, String group, short mode) {
		this.send(this._folderCreated(path, owner, group, mode));
	}

	@Override
	public void folderAdjusted(String path, String owner, String group, short mode) {
		this.send(this._folderAdjusted(path, owner, group, mode));
	}

	@Override
	public void fileDeleted(String path) {
		this.send(this._fileDeleted(path));
	}

	@Override
	public void copyStarted(String path) {
		this.send(this._copyStarted(path));
	}

	@Override
	public void fileCopied(String path, String owner, String group, short mode, long size, long modTime) {
		this.send(this._fileCopied(path, owner, group, mode, size, modTime));
	}

	@Override
	public void fileAdjusted(String path, String owner, String group, short mode) {
		this.send(this._fileAdjusted(path, owner, group, mode));
	}

	@Override
	public void error(String path, String message, Throwable t) {
		this.send(this._error(path, message, t));
	}

	@Override
	public void close() {
		this.producer.close();
	}
	

}
