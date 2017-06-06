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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
		log.debug("\nHDFS: " + hdfsTree.toString());
		Tree localTree = new LocalTree(parameters.getLocalPath());
		log.debug("\nLocal: " + localTree.toString());
		localTree.adjustPermissions(parameters.getOwner(), parameters.getGroup(), parameters.getFileMode(), parameters.getFolderMode());
		
		TreeDiff treeDiff = new TreeDiff(localTree, hdfsTree);
		
		if (Utils.hasText(parameters.getReportFile())) {
			Writer out = null;
			try {
				out = new BufferedWriter(new FileWriter(parameters.getReportFile(), false));
				out.write(parameters.toYaml());
				treeDiff.toYaml(out);
			} finally {
				if (out != null) {
					out.close();
				}
			}
			log.info(String.format("Report file:'%s' has been generated", parameters.getReportFile()));
		}
		if(!parameters.isDryRun()) {
			// --------------- First, cleanup dirty files
			for(Tree.File file : treeDiff.getFilesToDelete()) {
				Path path = concatPath(hdfsTree.root, file.path);
				log.info(String.format("Will delete file '%s'", path.toString()));
				fs.delete(path, false);
			}
			// ---------------- First, adjust folders
			for(Tree.Folder folder : treeDiff.getFoldersToAdjust()) {
				Path path = concatPath(hdfsTree.root, folder.path);
				fs.setPermission(path, new FsPermission(folder.mode));
				fs.setOwner(path, folder.owner, folder.group);
			}
			// ---------------- Now, create folder, in outer -> inner order, as folder or lexically ordered
			for(Tree.Folder folder : treeDiff.getFoldersToCreate()) {
				Path path = concatPath(hdfsTree.root, folder.path);
				fs.mkdirs(path, new FsPermission(folder.mode));
				fs.setOwner(path, folder.owner, folder.group);
			}
		}
		
		return 0;
	}
	
	static private Path concatPath(String root, String path) {
		return new Path(StringUtils.replace(root + "/" + path, "//", "/"));
	}
 	
}
