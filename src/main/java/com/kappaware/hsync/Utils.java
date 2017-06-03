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
package com.kappaware.hsync;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class Utils {

	public static boolean isNullOrEmpty(String s) {
		return (s == null || s.trim().length() == 0);
	}

	public static boolean hasText(String s) {
		return (s != null && s.trim().length() > 0);
	}

	public static boolean isTextEqual(String s1, String s2) {
		if (isNullOrEmpty(s1)) {
			return isNullOrEmpty(s2);
		} else {
			return s1.equals(s2);
		}
	}

	public static boolean isTextDifferent(String s1, String s2) {
		return !isTextEqual(s1, s2);
	}


	
	static public void dumpConfiguration(Configuration conf, String dumpFile) throws IOException {
		Writer out = null;
		try {
			out = new BufferedWriter(new FileWriter(dumpFile, false));
			/*
			Map<String, String> result = conf.getValByRegex(".*");
			for (String s : result.keySet()) {
				out.write(String.format("%s -> %s\n", s, result.get(s)));
			}
			*/
			Iterator<Map.Entry<String, String>> it = conf.iterator();
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				out.write(String.format("%s -> %s\n", entry.getKey(), entry.getValue()));
			}
			//Configuration.dumpConfiguration(conf, out);
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	public static boolean isEqual(Object o1, Object o2) {
		if (o1 == null) {
			return o2 == null;
		} else {
			return o1.equals(o2);
		}
	}

	/**
	 * This is just as isDifferent(...) is far more readable than !isEqual(...)
	 * @param o1
	 * @param o2
	 * @return
	 */
	public static boolean isDifferent(Object o1, Object o2) {
		return !isEqual(o1, o2);
	}

}
