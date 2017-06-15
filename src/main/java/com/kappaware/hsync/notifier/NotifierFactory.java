package com.kappaware.hsync.notifier;

import java.util.List;
import java.util.Vector;

import org.apache.hadoop.fs.Path;

import com.kappaware.hsync.config.ConfigurationException;

/**
 * Allow parsing of string as a notification target. As
 * 
 * kafka://broker1:9092,broker2:9092,.../topic?format=json&prop1=val1...
 * or:
 * logs://debug
 * or
 * logs://info
 * 
 * format is optional (JSON by default). Properties are used as Kafka consumers properties
 * 
 * @author sa
 * @throws ConfigurationException 
 *
 */
public class NotifierFactory {

	static private Notifier newNotifier(Path root, String clientId, String desc) throws ConfigurationException {
		if (desc.startsWith("logs://debug")) {
			return new DebugNotifier(root, clientId);
		} else if (desc.startsWith("logs://info")) {
			return new InfoNotifier(root, clientId);
		} else if (desc.startsWith("kafka://")) {
			return new KafkaNotifier(root, clientId, desc);
		} else {
			throw new ConfigurationException(String.format("Unreconized notification sink '%s'", desc));
		}
	}

	static public List<Notifier> newNotifierList(Path root, String clientId, List<String> descs) throws ConfigurationException {
		List<Notifier> notifiers = new Vector<Notifier>(descs.size());
		for (String desc : descs) {
			notifiers.add(newNotifier(root, clientId, desc));
		}
		return notifiers;
	}

}
