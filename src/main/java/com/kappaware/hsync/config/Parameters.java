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
	private List<String> reportFiles;
	private List<String> configFiles;
	private boolean dryRun;
	private List<String> notifySinks;
	private String owner;
	private String group;
	private Short fileMode;
	private Short folderMode;
	private Integer threadCount;
	private String clientId;
	private List<String> excludes;

	static OptionParser parser = new OptionParser();
	static {
		parser.formatHelpWith(new BuiltinHelpFormatter(120, 2));
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

	static OptionSpec<String> OWNER_OPT = parser.accepts("owner", "owner of target files (Default: source one)").withRequiredArg().describedAs("user").ofType(String.class);
	static OptionSpec<String> GROUP_OPT = parser.accepts("group", "group of target files (Default: source one)").withRequiredArg().describedAs("group").ofType(String.class);
	static OptionSpec<String> FILE_MODE_OPT = parser.accepts("fileMode", "mode of target files (Default: source one)").withRequiredArg().describedAs("0XXX").ofType(String.class);
	static OptionSpec<String> FOLDER_MODE_OPT = parser.accepts("folderMode", "mode of target folders (Default: source one)").withRequiredArg().describedAs("0XXX").ofType(String.class);

	static OptionSpec<Integer> THREADS_COUNT_OPT = parser.accepts("threads", "Number of acting threads").withRequiredArg().describedAs("<threadCount>").ofType(Integer.class).defaultsTo(1);
	static OptionSpec<String> CLIENT_ID_OPT = parser.accepts("clientId", "Client identifier").withRequiredArg().describedAs("<clientId>").ofType(String.class).defaultsTo("hsync");
	static OptionSpec<String> EXCLUDES_OPT = parser.accepts("excludes", "Pattern of file to exclude").withRequiredArg().describedAs("filePattern").ofType(String.class);

	
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
			this.reportFiles = result.valuesOf(REPORT_FILE_OPT);
			this.configFiles = result.valuesOf(CONFIG_FILES_OPT);
			this.dryRun = result.has(DRY_RUN_OPT);
			this.notifySinks = result.valuesOf(NOTIFY_SINKS_OPT);
			this.owner = result.valueOf(OWNER_OPT);
			this.group = result.valueOf(GROUP_OPT);
			this.threadCount = result.valueOf(THREADS_COUNT_OPT);
			this.clientId = result.valueOf(CLIENT_ID_OPT);
			this.excludes = result.valuesOf(EXCLUDES_OPT);
			if (result.has(FILE_MODE_OPT)) {
				this.fileMode = parseOctal(result.valueOf(FILE_MODE_OPT));
				if (this.fileMode == null || this.fileMode < 0 || this.fileMode > 0777) {
					throw new ConfigurationException(String.format("Invalid value for 'fileMode' parameter: '%s'", result.valueOf(FILE_MODE_OPT)));
				}
			}
			if (result.has(FOLDER_MODE_OPT)) {
				this.folderMode = parseOctal(result.valueOf(FOLDER_MODE_OPT));
				if (this.folderMode == null || this.folderMode < 0 || this.folderMode > 0777) {
					throw new ConfigurationException(String.format("Invalid value for 'folderMode' parameter: '%s'", result.valueOf(FOLDER_MODE_OPT)));
				}
			}
			if (!this.hdfsPath.startsWith("/") && !this.hdfsPath.startsWith("hdfs://")) {
				throw new ConfigurationException(String.format("'%s' is not an absolute HDFS path", this.hdfsPath));
			}
			if (!this.localPath.startsWith("/")) {
				throw new ConfigurationException(String.format("'%s' is not an absolute local path", this.localPath));
			}
			if (Utils.isNullOrEmpty(this.principal) ^ Utils.isNullOrEmpty(this.keytab)) {
				throw new ConfigurationException("Both or none of --principal and --keytab must be defined");
			}
		} catch (OptionException | MyOptionException t) {
			throw new ConfigurationException(usage(t.getMessage()));
		}
	}

	public String toYaml() {
		StringBuffer sb = new StringBuffer();
		sb.append("parameters:\n");
		sb.append(String.format("  hdfsPath: %s\n", N(this.hdfsPath)));
		sb.append(String.format("  localPath: %s\n", N(this.localPath)));
		sb.append(String.format("  principal: %s\n", N(this.principal)));
		sb.append(String.format("  keytab: %s\n", N(this.keytab)));
		sb.append(String.format("  dumpConfigFile: %s\n", N(this.dumpConfigFile)));
		sb.append(String.format("  reportFile: %s", L(this.reportFiles)));
		sb.append(String.format("  configFiles: %s", L(this.configFiles)));
		sb.append(String.format("  dryRun: %s\n", B(this.dryRun)));
		sb.append(String.format("  notifySinks: %s", L(this.notifySinks)));
		sb.append(String.format("  owner: %s\n", N(this.owner)));
		sb.append(String.format("  group: %s\n", N(this.group)));
		sb.append(String.format("  fileMode: %s\n", O(this.fileMode)));
		sb.append(String.format("  folderMode: %s\n", O(this.folderMode)));
		sb.append(String.format("  threadCount: %d\n", this.threadCount));
		sb.append(String.format("  clientId: %s\n", N(this.clientId)));
		sb.append(String.format("  excludes: %s", L(this.excludes)));
		return sb.toString();
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

	private static Short parseOctal(String s) {
		try {
			return Short.parseShort(s, 8);
		} catch (Throwable t) {
			return null;
		}
	}

	
	// -------------------------------------------------------------------------- Helper for Yaml stuff
	
	private static String N(String x) {
		if (x == null) {
			return "null";
		} else {
			return "\"" + x + "\"";
		}
	}

	private static String O(Short x) {
		if (x == null) {
			return "null";
		} else {
			return String.format("%04o", x);
		}
	}

	private static String B(boolean b) {
		return b ? "true" : "false";
	}

	private static String L(List<String> x) {
		if (x == null || x.size() == 0) {
			return "[]\n";
		} else {
			StringBuffer sb = new StringBuffer();
			sb.append("\n");
			for (String s : x) {
				sb.append("  - " + N(s) + "\n");
			}
			return sb.toString();
		}
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

	/**
	 * Being able to generate several was usefull for unit testing
	 * @return
	 */
	public List<String> getReportFiles() {
		return reportFiles;
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

	public String getOwner() {
		return owner;
	}

	public String getGroup() {
		return group;
	}

	public Short getFileMode() {
		return fileMode;
	}

	public Short getFolderMode() {
		return folderMode;
	}

	public Integer getThreadCount() {
		return threadCount;
	}

	public String getClientId() {
		return clientId;
	}

	public List<String> getExcludes() {
		return excludes;
	}

	
	
}
