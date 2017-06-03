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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hsync.config.ConfigurationException;
import com.kappaware.hsync.config.Parameters;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws IOException {
		try {
			System.exit(main2(argv));
		} catch (ConfigurationException e) {
			log.error(String.format("Error: %s", e.getMessage()));
			//System.err.println("ERROR: " + e.getMessage());
			System.exit(2);
		} catch (Throwable e) {
			log.error("Error in main():", e);
			//System.err.println("ERROR: " + e.getMessage());
			System.exit(2);
		}
	}

	static public int main2(String[] argv) throws ConfigurationException, IOException {
		log.info(String.format("hsync start..."));

		Parameters parameters = new Parameters(argv);
		Configuration config = new Configuration();
		for (String cf : parameters.getConfigFiles()) {
			File f = new File(cf);
			if (!f.canRead()) {
				throw new ConfigurationException(String.format("Unable to read file '%s'", cf));
			}
			log.debug(String.format("Will load '%s'", cf));
			config.addResource(new Path(cf));
		}
		//config.reloadConfiguration();
		if (Utils.hasText(parameters.getDumpConfigFile())) {
			Utils.dumpConfiguration(config, parameters.getDumpConfigFile());
		}
		if (Utils.hasText(parameters.getKeytab()) && Utils.hasText(parameters.getPrincipal())) {
			// Check if keytab file exists and is readable
			File f = new File(parameters.getKeytab());
			if (!f.canRead()) {
				throw new ConfigurationException(String.format("Unable to read keytab file: '%s'", parameters.getKeytab()));
			}
			UserGroupInformation.setConfiguration(config);
			if (!UserGroupInformation.isSecurityEnabled()) {
				throw new ConfigurationException("Security is not enabled in core-site.xml while Kerberos principal and keytab are provided.");
			}
			try {
				UserGroupInformation userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(parameters.getPrincipal(), parameters.getKeytab());
				UserGroupInformation.setLoginUser(userGroupInformation);
			} catch (Exception e) {
				throw new ConfigurationException(String.format("Kerberos: Unable to authenticate with principal='%s' and keytab='%s'.", parameters.getPrincipal(), parameters.getKeytab()));
			}
		}
		FileSystem fs = FileSystem.get(config);
		Tree hdfsTree = new HdfsTree(fs, parameters.getHdfsPath());
		System.out.println(hdfsTree.toString());
		
		Tree localTree = new LocalTree(parameters.getLocalPath());
		System.out.println(localTree.toString());
		/*
		YamlReport report = new YamlReport();

		if (Utils.hasText(parameters.getReportFile())) {
			Writer out = null;
			try {
				out = new BufferedWriter(new FileWriter(parameters.getReportFile(), false));
				out.write("# jdchive generated file.\n\n");
				String x = YamlUtils.yaml2String(report);
				out.write(x);
				//YamlUtils.writeYaml(out, report);
			} finally {
				if (out != null) {
					out.close();
				}
			}
			log.info(String.format("Report file:'%s' has been generated", parameters.getReportFile()));
		}
		*/
		return 0;
	}
}
