# Hsync

## Overview

Hsync is a java application aimed to transfer files from an Hadoop edge node to HDFS.

Is behavior is similar to rsync, as it run by comparing the content of a folder on the node to the content of a folder on HDFS and then:

- Copy file missing on HDFS

- Replace file on HDFS if they are different.

- Adjust rights on files on the HDFS side if relevant.

- Optionaly, issue notification of performed action to another subsystem.

We will call the source folder the 'landing zone', as it is typically a local space where file are downloaded from an external subsystem.

Hsync never delete files, either on HDFS nor on the landing zone.

To improve performance when copying a lot of input files, Hsync is able to parallelize several file operation.

## Parameters

Name | req? | 	Description
--- | ---  | ---
`--localPath`|yes|The source path on the local node. Must be a folder.
`--hdfsPath`|yes|The target HDFS path. Must be a folder.
`--principal`|no|The Kerberos principal. Required if HDFS is secured
`--keytab`|no|Path of the corresponding keytab file. Required if HDFS is secured
`--reportFile`|no|A path to a file which will be created by Hsync to report a summary of the performed operations. May be specified several times.
`--dryRun`|no|If set, then no effective operation will be performed.
`--notifier`|no|Sink to push notification on each copied file. See below for the syntax of this parameter.
`--owner`|no|Force the owner of the copied files on HDFS. Default to the source one.
`--group`|no|Force the group of the copied files on HDFS. Default to the source one.
`--fileMode`|no|Force the permissions of the copied files on HDFS. Default to the source one.
`--folderMode`|no|Force the permissions of the created folders on HDFS. Default to the source one.
`--threads`|no|Define the number of transfert operation to perform simultaneously. Default to 1.
`--clientId`|no|An identifier which will be inserted in notification.
`--exclude`|no|An expression to exclude some files on the landing zone. May be specified several times.
`--configFile`|no|Hadoop supplementary configuration files. (xxxx-site.xml). May be specified several times.<br>This is advanced parameter, to be used only in some specific cases.

## Wrapper script

A typical java program, it could be of great help to design a small wrapper script to ease the launch. Here is a sample.


```bash
# Pickup location of the script
MYDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG4J=-Dlog4j.configuration=file:${MYDIR}/log4j.xml

# Warning: Setting our jar first may be important, as embeded libraries will take precedences.
MY_CLASSPATH=$MYDIR/hsync_uber-0.1.0.jar:`hadoop classpath`

java -classpath  $MY_CLASSPATH ${LOG4J} com.kappaware.hsync.Main --reportFile /tmp/report.yml \
   --excludes "**/.*" --notifier "logs://info" "$@"
if [ $? -eq 0 ]
then
	cat /tmp/report.yml
fi
```
In this sample, we display a generated report file and exclude all files beginning with `.`. We also issue operation's notification in the logging system at INFO level.

Other parameters (At least `--localPath` and ``--hdfsPath`) will be provided on launch

It is of course assumed we are on an edge node configured as an Hadoop client.

For reference, here is a sample of log4j.xml:

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE log4j:configuration SYSTEM "log4j.dtd">
<log4j:configuration xmlns:log4j="http://jakarta.apache.org/log4j/">
	 <appender name="STDOUT" class="org.apache.log4j.ConsoleAppender">
    	<param name="Target" value="System.out" />
    	<layout class="org.apache.log4j.PatternLayout"><param name="ConversionPattern" value="[%d{ISO8601}] -5p %c:%L %m %n" /> </layout>
    </appender>
 	<logger name="com.kappaware"><level value="info"/></logger>
    <root>
      <level value="info" /> 
      <appender-ref ref="STDOUT" />
	</root>
</log4j:configuration>
```

## Notification

The `--notifier` parameter is a string which could be of three form:

- `logs://debug`: All notifications will be logged as DEBUG level message.

- `logs://info`: All notifications will be logged as INFO level message.

- `kafka://<brokers>/topic`: All notification will be send to a kafka topic in json form.

## kafka notification sink:

Here is an example of kafka notification parameter:

```
--notifier kafka://broker1.mycluster.com:9002,broker2.mycluster.com:9002,broker3.mycluster.com:9002/infiles
```
The notification themself will be JSON messages.

For example a typical file transfer will first issue a message when operation is beginning:

```json
{ 
  "clientId": "hsync", 
  "action":"COPY_STARTED",
  "path":"/tests/test03/folder1/subfolder1/file1.txt" 
}
```
And another one when tranfer is completed and file ready:

```json
{ 
  "clientId":"hsync", 
  "action":"FILE_COPIED", 
  "path":"/tests/test03/folder1/subfolder1/file1.txt", 
  "owner":"hdfs", "group":"hadoop", 
  "mode":420, 
  "size":41, 
  "modificationTime":1496780984000 
}
```
Of course, in most cases, only the second message will be used.

You can find a complete description of notification message [here](TODO)