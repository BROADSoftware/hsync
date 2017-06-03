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
package com.kappaware.hsync.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hsync.Utils;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class Parameters {
	static Logger log = LoggerFactory.getLogger(Parameters.class);
	
	private String hdfsPath;
	private String localPath;

	private String principal;
	private String keytab;
	private String dumpConfigFile;
	private String reportFile;
	private List<String> configFiles;
	private boolean dryRun;
	private List<String> notifySinks;

	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120,2));
	}	
	static OptionSpec<String> HDFS_PATH = parser.accepts("hdfsPath", "HDFS path").withRequiredArg().describedAs("/path/on/hdfs").ofType(String.class).required();
	static OptionSpec<String> LOCAL_PATH = parser.accepts("localPath", "Local path").withRequiredArg().describedAs("/path/on/this/node").ofType(String.class).required();

	static OptionSpec<String> CONFIG_FILES_OPT = parser.accepts("configFile", "Config file (xxx-site.xml). May be specified several times").withRequiredArg().describedAs("xxxx-site.xml").ofType(String.class);
	static OptionSpec<String> PRINCIPAL_OPT = parser.accepts("principal", "Kerberos principal").withRequiredArg().describedAs("principal").ofType(String.class);
	static OptionSpec<String> KEYTAB_OPT = parser.accepts("keytab", "Keytyab file path").withRequiredArg().describedAs("keytab_file").ofType(String.class);
	static OptionSpec<String> DUMP_CONFIG_FILE_OPT = parser.accepts("dumpConfigFile", "Debuging purpose: All HBaseConfiguration will be dumped in this file").withRequiredArg().describedAs("dump_file").ofType(String.class);
	static OptionSpec<String> REPORT_FILE_OPT = parser.accepts("reportFile", "Allow tracking of performed operation").withRequiredArg().describedAs("report_file").ofType(String.class);
	static OptionSpec<?> DRY_RUN_OPT = parser.accepts("dryRun", "Perform no action");
	static OptionSpec<String> NOTIFY_SINKS_OPT = parser.accepts("notifySinks", "Sink to push notification on each copied file").withRequiredArg().describedAs("<notificationSink>").ofType(String.class);

	
	@SuppressWarnings("serial")
	private static class MyOptionException extends Exception {
		public MyOptionException(String message) {
			super(message);
		}
	}

	
	public Parameters(String[] argv) throws ConfigurationException {
		try {
			OptionSet result = parser.parse(argv);
			if (result.nonOptionArguments().size() > 0 && result.nonOptionArguments().get(0).toString().trim().length() > 0) {
				throw new MyOptionException(String.format("Unknow option '%s'", result.nonOptionArguments().get(0)));
			}
			this.hdfsPath = result.valueOf(HDFS_PATH);
			this.localPath = result.valueOf(LOCAL_PATH);
			this.principal = result.valueOf(PRINCIPAL_OPT);
			this.keytab = result.valueOf(KEYTAB_OPT);
			this.dumpConfigFile = result.valueOf(DUMP_CONFIG_FILE_OPT);
			this.reportFile = result.valueOf(REPORT_FILE_OPT);
			this.configFiles = result.valuesOf(CONFIG_FILES_OPT);
			this.dryRun = result.has(DRY_RUN_OPT);
			this.notifySinks = result.valuesOf(NOTIFY_SINKS_OPT);
			if(!this.hdfsPath.startsWith("/") && !this.hdfsPath.startsWith("hdfs://")) {
				throw new ConfigurationException(String.format("'%s' is not an absolute HDFS path", this.hdfsPath));
			}
			if(!this.localPath.startsWith("/")) {
				throw new ConfigurationException(String.format("'%s' is not an absolute local path", this.localPath));
			}
			if(Utils.isNullOrEmpty(this.principal) ^ Utils.isNullOrEmpty(this.keytab)) {
				throw new ConfigurationException("Both or none of --principal and --keytab must be defined");
			}
		} catch (OptionException | MyOptionException t) {
			throw new ConfigurationException(usage(t.getMessage()));
		}
	}

	private static String usage(String err) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(baos);
		if (err != null) {
			pw.print(String.format("\n\n * * * * * ERROR: %s\n\n", err));
		}
		try {
			parser.printHelpOn(pw);
		} catch (IOException e) {
		}
		pw.flush();
		pw.close();
		return baos.toString();
	}

	// --------------------------------------------------------------------------

	public String getHdfsPath() {
		return hdfsPath;
	}

	public String getLocalPath() {
		return localPath;
	}

	public String getPrincipal() {
		return principal;
	}

	public String getKeytab() {
		return keytab;
	}

	public String getDumpConfigFile() {
		return dumpConfigFile;
	}

	public String getReportFile() {
		return reportFile;
	}

	public List<String> getConfigFiles() {
		return configFiles;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public List<String> getNotifySinks() {
		return notifySinks;
	}

	
}
