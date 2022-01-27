/**
 * 
 */
package com.ksign.wizlook.bigdata.collector.config;

/**
 * Collector Config 클래스
 */
public interface Config {
	// ### manage DB config ###
	public static final String CONSOLE_H2_URL 					= "console.h2.url";
	public static final String CONSOLE_H2_USER 					= "console.h2.user";
	public static final String CONSOLE_H2_PASSWORD 				= "console.h2.password";

	public static final String COLLECTOR_H2_URL 				= "collector.h2.url";
	public static final String COLLECTOR_H2_USER 				= "collector.h2.user";
	public static final String COLLECTOR_H2_PASSWORD 			= "collector.h2.password";

	public static final String CONSOLE_HOST 					= "console.host";
	public static final String CONSOLE_RPC_PORT 				= "console.rpc.port";

	// ### history DB for dashboard(influxdb) config ###
	public static final String LOG_DB_ENABLED					= "log.db.enabled";
	public static final String LOG_DB_URL 						= "log.db.url";
	public static final String LOG_DB_USERNAME 					= "log.db.username";
	public static final String LOG_DB_PASSWORD 					= "log.db.password";
	public static final String LOG_DB_NAME 						= "log.db.name";
	public static final String LOG_DB_TABLE_NAME 				= "log.db.table.name";
	public static final String LOG_DB_BULK_INSERT_SIZE 			= "log.db.bulk.insert.size";

	// ### collect config ###
	public static final String COLLECT_DIR 						= "collect.dir";
	public static final String COLLECT_ERROR_BASE_DIR 			= "collect.error.base.dir";
	public static final String COLLECT_METAINFO_BASE_DIR 		= "collect.metainfo.base.dir";
	public static final String COLLECT_LAST_MODIFIED_FILE 		= "collect.last.modified.file";
	public static final String POLL_SCHEDULER_THREAD_COUNT 		= "poll.scheduler.thread.count";
	public static final String PUSH_RECEIVE_BUFFER_SAVE_ENABLED  = "push.receive.buffer.save.enabled";
	public static final String PUSH_RECEIVE_BUFFER_SAVE_INTERVAL = "push.receive.buffer.save.interval.millis";

	// ### collect jdbc config ###
	public static final String JDBC_LOGIN_TIMEOUT_SEC			= "jdbc.login.timeout.sec";
	public static final String JDBC_FETCH_SIZE 					= "jdbc.fetch.size";

	// ### collect history config ###
	public static final String COLLECT_LOGGING_ENABLED			= "collect.logging.enabled";
	public static final String COLLECT_LOGGING_BASE_DIR			= "collect.logging.base.dir";
	public static final String COLLECT_LOGGING_RETENTION_PERIOD	= "collect.logging.retention.period.day";

	// ### collect file encoding config ###
	public static final String COLLECT_FILE_ENCODING_CONVERT 		 	= "collect.file.encoding.convert";
	public static final String COLLECT_FILE_ENCODING_CONVERT_TYPE 	 	= "collect.file.encoding.convert.type";
	public static final String COLLECT_FILE_ENCODING_CONVERT_SHELL_PATH = "collect.file.encoding.convert.shell.path";

	// ### backup config ###
	public static final String BACKUP_LOG_ENABLED				= "backup.log.enabled";
	public static final String BACKUP_LOG_BASE_DIR 				= "backup.log.base.dir";
	public static final String BACKUP_LOG_RETENTION_PERIOD 		= "backup.log.retention.period.day";

	public static final String AGENT_RECEIVER_ENABLED			= "agent.receiver.enabled";
	public static final String AGENT_RECEIVER_IMPL_CLASS_PATH 	= "agent.receiver.impl.class.path";
	public static final String AGENT_RECEIVER_PORT 				= "agent.receiver.port";

	// ### send log to node Config ###
	public static final String SEND_LOG_ENABLED					= "send.log.enabled";
	public static final String SEND_LOG_DIR 					= "send.log.dir";

	// ### interface config ###
	public static final String INTERFACE_PORT 					= "interface.port";

	public static final String SSL_ENABLED 						= "ssl.enabled";
	public static final String SSL_PROTOCOL 					= "ssl.protocol";
	public static final String SSL_KEYSTORE_FILE 				= "ssl.keystore.file";
	public static final String SSL_KEYSTORE_PWD					= "ssl.keystore.pwd";

	public static final String CERT_CERTIFICATE					= "cert.certificate";
	public static final String CERT_PRIVATEKEY					= "cert.privatekey";
	public static final String CERT_PRIVATEKEY_PWD				= "cert.privatekey.pwd";
	public static final String KSIGN_CRYPTO_FOR_JAVA_PATH		= "ksigncrypto.for.java.path";

	public static final String LICENSE_PATH						= "license.path";

	public String get( String key );

	public int getInt( String key );

	public long getLong( String key );

	public boolean getBoolean( String key );
}