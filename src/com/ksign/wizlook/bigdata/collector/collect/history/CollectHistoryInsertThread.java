package com.ksign.wizlook.bigdata.collector.collect.history;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.AbstractCollect;
import com.ksign.wizlook.bigdata.collector.log.FileRollingLogger;
import com.ksign.wizlook.bigdata.collector.log.LoggerManager;
import com.ksign.wizlook.bigdata.collector.util.H2ConnectionManager;

/**
 * File에 저장되어 있는 수집 이력을 H2 DB에 Insert/update 하기 위한 Thread 클래스
 * @author byw
 */
public class CollectHistoryInsertThread implements Runnable {
	/** collectHistoryLogger append file name */
	private final String appendFilePath;
	/** collectHistoryLogger rolling directory */
	private final File rollingDir;
	/** log4j2 logger */
	private Logger logger = LogManager.getLogger();
	/** collect history data insert query */
	private String insertQuery;
	/** collect history data merge query */
	private String updateQuery;

	public CollectHistoryInsertThread() throws RuntimeException, ConnectException, IOException, SQLException {
		FileRollingLogger fileRollingLogger = LoggerManager.getInstance().getFileRollingLogger("collectHistoryLogger");
		if(fileRollingLogger == null) throw new RuntimeException("Not found 'collectHistoryLogger'.");
		this.appendFilePath = fileRollingLogger.getAppendFilePath();
		this.rollingDir = new File(fileRollingLogger.getRollingFilePath()).getParentFile();
		initHistoryTable();
		initQuery();
	}

	/**
	 * 테이블 및 인덱스 생성
	 * @throws SQLException
	 */
	private void initHistoryTable() throws SQLException {
		String createTableQuery = "CREATE CACHED TABLE IF NOT EXISTS PUBLIC.TBL_COLLECT_HISTORY ( " +
													   " SESSION_ID VARCHAR, " +
													   " START_DATE VARCHAR, " +
								  					   " END_DATE VARCHAR, " +
								  					   " LOGPOLICY_ID VARCHAR, " +
								  					   " DATA_SOURCE_ID VARCHAR, " +
								  					   " ELAPSED_TIME BIGINT(19), " +
								  					   " STATUS VARCHAR, " +
								  					   " LOG_SIZE BIGINT(19), " +
								  					   " START_DATE_DAY VARCHAR ) ";

		String createIndexQuery = "CREATE INDEX IF NOT EXISTS HISTORY_IDX01 ON TBL_COLLECT_HISTORY(START_DATE)";
		Connection conn = null;
		try {
			conn = H2ConnectionManager.INSTANCE.getCollectorConnection();
			conn.createStatement().execute(createTableQuery);
			conn.createStatement().execute(createIndexQuery);
			conn.commit();
		} catch(SQLException e) {
			throw e;
		} finally {
			if(conn != null) try { conn.close(); } catch (SQLException e) {}
		}
	}

	/**
	 * 인서트 및 업데이트 쿼리 초기화
	 */
	private void initQuery() {
		insertQuery = new StringBuilder().append(" INSERT INTO TBL_COLLECT_HISTORY		  		 ")
										 .append("			  ( SESSION_ID, 					 ")
										 .append("				START_DATE, 					 ")
										 .append("				END_DATE, 						 ")
										 .append("				LOGPOLICY_ID, 					 ")
										 .append("				DATA_SOURCE_ID, 				 ")
										 .append("				STATUS, 						 ")
										 .append("				ELAPSED_TIME,					 ")
										 .append("				LOG_SIZE,						 ")
										 .append("				START_DATE_DAY )			 	 ")
										 .append(" VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? ) ").toString();

		updateQuery = new StringBuilder().append(" MERGE INTO  TBL_COLLECT_HISTORY 			")
										 .append("		      ( SESSION_ID,					")
										 .append("				START_DATE, 				")
										 .append("				END_DATE, 					")
										 .append("				LOGPOLICY_ID, 				")
										 .append("				DATA_SOURCE_ID, 			")
										 .append("				STATUS, 					")
										 .append("				ELAPSED_TIME,				")
										 .append("				LOG_SIZE,					")
										 .append("				START_DATE_DAY )			")
										 .append("		 KEY(SESSION_ID) 					")
										 .append("		 VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ? ) ").toString();
	}

	@Override
	public void run() {
		// 1. 로그가 저장된 디렉토리 스캔 ( rolling 된 파일이 타겟이 됨 )
		// 2. 스캔된 파일을 읽어 내용을 db에 적재
		// 3. 파일 삭제
		logger.info("	Start " + this.getClass().getSimpleName());
		while(!Thread.currentThread().isInterrupted()) {
			File[] fileList = rollingDir.listFiles(new FileFilter(){
				@Override
				public boolean accept(File file) {
					if(!file.isFile() || file.getName().equalsIgnoreCase(new File(appendFilePath).getName())) return false;
					else return true;
				}
			});

			if(fileList == null || fileList.length == 0) {
				try { Thread.sleep(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
				continue;
			} else {
				// rolling중일 수도 있으니 10ms만 기다렸다가 file을 open. ( windows에서 문제의 소지가 있어 추가 )
				try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			}

			for(File file : fileList) {
				Connection conn = null;
				PreparedStatement insertPstmt = null;
				PreparedStatement updatePstmt = null;
				BufferedReader reader = null;
				boolean result = false;

				try {
					conn = H2ConnectionManager.INSTANCE.getCollectorConnection();
					conn.setAutoCommit(true);
					insertPstmt = conn.prepareStatement(insertQuery.toString());
					updatePstmt = conn.prepareStatement(updateQuery.toString());
					String readLine = null;
					reader = new BufferedReader(new FileReader(file));
					while((readLine = reader.readLine()) != null) {
						if(readLine.length() < 1) continue;
						String[] valueArr = readLine.split(AbstractCollect.COLLECTOR_SEPARATOR, 9);
						int idx = 1;
						String type = valueArr[0];
						PreparedStatement pstmt = null;
						if(CollectLogger.COLLECT_LOGGING_START.equals(type)) {
							pstmt = insertPstmt;
						} else if(CollectLogger.COLLECT_LOGGING_END.equals(type)){
							pstmt = updatePstmt;
						} else {
							logger.error(this.getClass().getSimpleName() + " :: Is invalid COLLECT_LOGGING_TYPE. value=[" + type + "]");
							continue;
						}
						String sessionId = valueArr[1];
						String startDate = valueArr[2];
						String endDate = valueArr[3];
						String logpolicyId = valueArr[4];
						String dataSourceId = valueArr[5];
						String status = valueArr[6];
						String elapsedTime = valueArr[7];
						String logSize = valueArr[8];

						try { 
							pstmt.setString(idx++, sessionId);
							pstmt.setString(idx++, startDate);
							if(Strings.isNullOrEmpty(endDate)) pstmt.setNull(idx++, Types.VARCHAR); 
							else pstmt.setString(idx++, endDate);
							pstmt.setString(idx++, logpolicyId);
							pstmt.setString(idx++, dataSourceId);
							pstmt.setString(idx++, status);
							if(Strings.isNullOrEmpty(elapsedTime)) pstmt.setNull(idx++, Types.BIGINT); 
							else pstmt.setLong(idx++, Long.parseLong(elapsedTime));
							if(Strings.isNullOrEmpty(logSize)) pstmt.setNull(idx++, Types.BIGINT); 
							else pstmt.setLong(idx++, Long.parseLong(logSize));
							pstmt.setString(idx++, startDate.substring(0, 8));
							pstmt.executeUpdate();
							pstmt.clearParameters();
						} catch (SQLException e) {
							logger.error(this.getClass().getSimpleName(), e);
						}
					}
					result = true;
				} catch (SQLException e) {
					logger.error(this.getClass().getSimpleName(), e);
				} catch (IOException e) {
					logger.error(this.getClass().getSimpleName(), e);
				} catch (Exception e) { 
					logger.error(this.getClass().getSimpleName(), e);
				} finally {
					if(result) { try { conn.commit(); } catch (SQLException e) {} } else { try { conn.rollback(); } catch (SQLException e) {} }
					if(reader != null) try { reader.close(); } catch (IOException e) {}
					if(insertPstmt != null) try { insertPstmt.close(); } catch (SQLException e) {}
					if(updatePstmt != null) try { updatePstmt.close(); } catch (SQLException e) {}
					if(conn != null) try { conn.close(); } catch (SQLException e) {}
					file.delete();
				}
			}
		}
		logger.info("	Stop " + this.getClass().getSimpleName());
	}
}