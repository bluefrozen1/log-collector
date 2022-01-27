/**
 * 
 */
package com.ksign.wizlook.bigdata.collector.util;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.jdbcx.JdbcConnectionPool;

import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * H2 Connection Pool 관리 클래스
 */
public enum H2ConnectionManager {
	/** singleton instance */
	INSTANCE;
	/** H2 JDBC connection pool */
	private JdbcConnectionPool collectorConnectionPool = null;

	private H2ConnectionManager() {
		Config config = ConfigLoader.getInstance();
		collectorConnectionPool = JdbcConnectionPool.create( config.get(Config.COLLECTOR_H2_URL), config.get(Config.COLLECTOR_H2_USER), config.get(Config.COLLECTOR_H2_PASSWORD) );
	}

	public Connection getCollectorConnection() throws SQLException {
		return collectorConnectionPool.getConnection();
	}

	public void dispolse() {
		collectorConnectionPool.dispose();
	}
}