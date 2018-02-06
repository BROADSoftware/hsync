/*
 * Copyright (C) 2017 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.hsync.notifier;

import java.util.List;
import java.util.Vector;


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

	static private Notifier newNotifier(String clientId, String desc) throws ConfigurationException {
		if (desc.startsWith("logs://debug")) {
			return new DebugNotifier(clientId);
		} else if (desc.startsWith("logs://info")) {
			return new InfoNotifier(clientId);
		} else if (desc.startsWith("kafka://")) {
			return new KafkaNotifier(clientId, desc);
		} else {
			throw new ConfigurationException(String.format("Unreconized notification sink '%s'", desc));
		}
	}

	static public List<Notifier> newNotifierList(String clientId, List<String> descs) throws ConfigurationException {
		List<Notifier> notifiers = new Vector<Notifier>(descs.size());
		for (String desc : descs) {
			notifiers.add(newNotifier(clientId, desc));
		}
		return notifiers;
	}

}
