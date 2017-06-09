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
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.hsync.config.ConfigurationException;
import com.kappaware.hsync.config.Parameters;
import com.kappaware.hsync.notifier.DebugNotifier;
import com.kappaware.hsync.notifier.Notifier;

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
		if (!parameters.isDryRun()) {
			Notifier notifier = new DebugNotifier(new Path(hdfsTree.root), parameters.getClientId());
			// --------------- First, cleanup dirty files
			for (Tree.File file : treeDiff.getFilesToDelete()) {
				Path path = Utils.concatPath(hdfsTree.root, file.path);
				fs.delete(path, false);
				notifier.fileDeleted(path);
			}
			// ---------------- First, adjust folders
			for (Tree.Folder folder : treeDiff.getFoldersToAdjust()) {
				Path path = Utils.concatPath(hdfsTree.root, folder.path);
				fs.setPermission(path, new FsPermission(folder.mode));
				fs.setOwner(path, folder.owner, folder.group);
				notifier.folderAdjusted(path, folder.owner, folder.group, folder.mode);
			}
			// ---------------- Now, create folder, in outer -> inner order, as folder or lexically ordered
			for (Tree.Folder folder : treeDiff.getFoldersToCreate()) {
				Path path = Utils.concatPath(hdfsTree.root, folder.path);
				fs.mkdirs(path, new FsPermission(folder.mode));
				fs.setOwner(path, folder.owner, folder.group);
				notifier.folderCreated(path, folder.owner, folder.group, folder.mode);
			}
			// Create and fed up the queue.
			Queue<FileAction> queue = new ConcurrentLinkedQueue<FileAction>();
			for (Tree.File file : treeDiff.getFilesToAdjust()) {
				queue.add(new FileAction(FileAction.Type.ADJUST, file));
			}
			for (Tree.File file : treeDiff.getFilesToReplace()) {
				queue.add(new FileAction(FileAction.Type.REPLACE, file));
			}
			for (Tree.File file : treeDiff.getFilesToCreate()) {
				queue.add(new FileAction(FileAction.Type.COPY, file));
			}
			// And create consuming thread
			List<FileThread> fileThreads = new Vector<FileThread>();
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					log.debug("Shutdown hook called!");
					for (FileThread ft : fileThreads) {
						ft.stopRunning();
					}
				}
			});
			for (int i = 0; i < parameters.getThreadCount(); i++) {
				fileThreads.add(new FileThread(i, fs, localTree.root, hdfsTree.root, queue, notifier));
			}
			waitCompletion(fileThreads);
			Utils.sleep(100); // To let message to be drained
		}
		return 0;
	}

	private static void waitCompletion(List<FileThread> fileThreads) {
		int fileCount = 0;
		long volume = 0;
		for (FileThread ft : fileThreads) {
			try {
				ft.join();
				fileCount += ft.getFileCount();
				volume += ft.getVolume();
				log.info(String.format("Thread#%d: %d files handled, for a volume of %d KBytes", ft.getSlot(), ft.getFileCount(), ft.getVolume() / 1024));
			} catch (InterruptedException e) {
				log.debug(String.format("Interrupted in join of FileThread#%d", ft.getSlot()));
			}
		}
		log.info(String.format("TOTAL: %d files handled, for a volume of %d KBytes (%d MBytes)", fileCount, volume / 1024, volume / (1024 * 1024)));
	}

}
