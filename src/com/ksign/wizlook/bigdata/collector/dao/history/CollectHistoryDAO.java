package com.ksign.wizlook.bigdata.collector.dao.history;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.util.H2ConnectionManager;

/**
 * 수집 이력 조회 관련 DAO 클래스
 * @author byw
 */
public enum CollectHistoryDAO {
	INSTANCE;
	private final Logger logger = LogManager.getLogger();

	/**
	 * 수집 이력 조회
	 * @param logpolicyId 정책아이디
	 * @param dataSourceId 데이터소스아이디
	 * @param startDate 수집시작날짜 ( yyyyMMddHHmmssSSS )
	 * @param endDate 수집완료날짜 ( yyyyMMddHHmmssSSS )
	 * @param status 수집 상태 ( SUCCESS / RUNNING / KILLED / ERROR )
	 * @param limit 요청 limit
	 * @param offset 요청 offset
	 * @return 수집 이력
	 * @throws CollectException 수집 이력 조회 도중 SQLException 발생
	 */
	public List<Map<String, String>> getCollectHistoryList(String logpolicyId, String dataSourceId,
			String startDate, String endDate, String status, int limit, int offset) throws CollectException {
		List<Map<String, String>> resultMapList = new ArrayList<Map<String, String>>();
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append(" SELECT  SESSION_ID, START_DATE, END_DATE, LOGPOLICY_ID,  ")
						.append("		  DATA_SOURCE_ID, ELAPSED_TIME, STATUS, LOG_SIZE ")
						.append(" FROM    TBL_COLLECT_HISTORY ")
						.append(" WHERE   1 = 1 ");
			if(!Strings.isNullOrEmpty(logpolicyId)) queryBuilder.append(" AND LOGPOLICY_ID = '").append(logpolicyId.toString()).append("'");
			if(!Strings.isNullOrEmpty(dataSourceId)) queryBuilder.append(" AND DATA_SOURCE_ID = '").append(dataSourceId.toString()).append("'");
			if(!Strings.isNullOrEmpty(startDate)) queryBuilder.append(" AND START_DATE >= '").append(startDate.toString()).append("'");
			if(!Strings.isNullOrEmpty(endDate)) queryBuilder.append(" AND START_DATE <= '").append(endDate.toString()).append("'");
			if(!Strings.isNullOrEmpty(status)) queryBuilder.append(" AND STATUS = '").append(status.toString()).append("'");
			queryBuilder.append(" ORDER BY START_DATE DESC ");
			if(limit > 0) queryBuilder.append(" LIMIT ").append(limit);
			if(offset > 0) queryBuilder.append(" OFFSET ").append(offset);

			conn = H2ConnectionManager.INSTANCE.getCollectorConnection();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(queryBuilder.toString());

			while(rs.next()) {
				Map<String, String> resultMap = new HashMap<String, String>();
				resultMap.put("sessionId", rs.getString("SESSION_ID"));
				resultMap.put("startDate", rs.getString("START_DATE"));
				resultMap.put("endDate", rs.getString("END_DATE"));
				resultMap.put("logpolicyId", rs.getString("LOGPOLICY_ID"));
				resultMap.put("dataSourceId", rs.getString("DATA_SOURCE_ID"));
				resultMap.put("elapsedTime", rs.getString("ELAPSED_TIME"));
				resultMap.put("status", rs.getString("STATUS"));
				resultMap.put("logSize", rs.getString("LOG_SIZE"));
				resultMapList.add(resultMap);
			}
		} catch(SQLException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_LOAD_HISTORY, e);
		} finally {
			if(rs != null) try { rs.close(); } catch(SQLException e) {}
			if(stmt != null) try { stmt.close(); } catch(SQLException e) {}
			if(conn != null) try { conn.close(); } catch(SQLException e) {}
		}
		return resultMapList;
	}

	/**
	 * 수집 이력 총 건수 조회
	 * @param logpolicyId 정책아이디
	 * @param dataSourceId 데이터소스아이디
	 * @param startDate 수집시작날짜 ( yyyyMMddHHmmssSSS )
	 * @param endDate 수집완료날짜 ( yyyyMMddHHmmssSSS )
	 * @param status 수집 상태 ( SUCCESS / RUNNING / KILLED / ERROR )
	 * @return 수집 이력 총 건수 조회
	 * @throws CollectException 수집 이력 건수 조회 도중 SQLException 발생
	 */
	public long getCollectHistoryCount(String logpolicyId, String dataSourceId,
			String startDate, String endDate, String status) throws CollectException {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append(" SELECT  COUNT(*)			  ")
						.append(" FROM    TBL_COLLECT_HISTORY ")
						.append(" WHERE   1 = 1 			  ");
			if(!Strings.isNullOrEmpty(logpolicyId)) queryBuilder.append(" AND LOGPOLICY_ID = '").append(logpolicyId.toString()).append("'");
			if(!Strings.isNullOrEmpty(dataSourceId)) queryBuilder.append(" AND DATA_SOURCE_ID = '").append(dataSourceId.toString()).append("'");
			if(!Strings.isNullOrEmpty(startDate)) queryBuilder.append(" AND START_DATE >= '").append(startDate.toString()).append("'");
			if(!Strings.isNullOrEmpty(endDate)) queryBuilder.append(" AND START_DATE <= '").append(endDate.toString()).append("'");
			if(!Strings.isNullOrEmpty(status)) queryBuilder.append(" AND STATUS = '").append(status.toString()).append("'");

			conn = H2ConnectionManager.INSTANCE.getCollectorConnection();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rs = stmt.executeQuery(queryBuilder.toString());

			if(rs.next()) return rs.getLong(1);
		} catch(SQLException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_LOAD_HISTORY, e);
		} finally {
			if(rs != null) try { rs.close(); } catch(SQLException e) {}
			if(stmt != null) try { stmt.close(); } catch(SQLException e) {}
			if(conn != null) try { conn.close(); } catch(SQLException e) {}
		}
		return 0;
	}

	/**
	 * 수집 상세로그 조회
	 * 수집 상세 이력은 파일로 저장되며 경로는 collect.logging.base.dir/logpolicyId/dataSourceId/yyyyMMdd/sessionId.log 이다.
	 * @param logpolicyId 정책아이디
	 * @param dataSourceId 데이터소스아이디
	 * @param sessionId 세션아이디
	 * @return 수집상세이력
	 * @throws CollectException 상세이력 조회 도중 SQLException 발생, 로그가 존재 안할 경우 CollectorException 발생
	 */
	public String getCollectHistoryDetail(String logpolicyId, String dataSourceId, String sessionId) throws CollectException {
		BufferedReader reader = null;
		try {
			String logDate = getLogDate(logpolicyId, dataSourceId, sessionId);
			StringBuilder filePathBuilder = new StringBuilder();
			filePathBuilder.append(ConfigLoader.getInstance().get(Config.COLLECT_LOGGING_BASE_DIR))
						   .append(File.separator).append(logpolicyId.toString())
						   .append(File.separator).append(dataSourceId.toString())
						   .append(File.separator).append(logDate)
						   .append(File.separator).append(sessionId).append(".log");
			File file = new File(filePathBuilder.toString());
			StringBuilder detailLog = new StringBuilder();
			if(file.exists()) {
				reader = new BufferedReader(new FileReader(file));
				String lineStr = null;
				boolean firstLine = true;
				while((lineStr = reader.readLine()) != null) {
					if(firstLine) {
						firstLine = false;
					} else {
						detailLog.append(System.lineSeparator());
					}
					detailLog.append(lineStr);
				}
			}
			return detailLog.toString();
		} catch(IOException e) {
			logger.error(this.getClass().getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_LOAD_HISTORY);
		} catch(CollectException e) {
			logger.error(this.getClass().getClass().getSimpleName(), e);
			throw e;
		} finally {
			if(reader != null) try { reader.close(); } catch(IOException e) {}
		}
	}

	/**
	 * sessionId를 통하여 상세로그의 날짜 조회
	 * @param logpolicyId 정책아이디
	 * @param dataSourceId 데이터소스아이디
	 * @param sessionId 세션아이디
	 * @throws CollectException Db조회 시 SQLException발생 시, 수집 이력이 존재하지 않을 시
	 */
	private String getLogDate(String logpolicyId, String dataSourceId, String sessionId) throws CollectException {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			StringBuilder queryBuilder = new StringBuilder();
			queryBuilder.append(" SELECT START_DATE 			")
						.append(" FROM   TBL_COLLECT_HISTORY	")
						.append(" WHERE  LOGPOLICY_ID = '").append(logpolicyId.toString()).append("'")
						.append("   AND  DATA_SOURCE_ID = '").append(dataSourceId.toString()).append("'")
						.append("   AND  SESSION_ID = '").append(sessionId.toString()).append("'");
			conn = H2ConnectionManager.INSTANCE.getCollectorConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(queryBuilder.toString());
			if(rs.next()) return rs.getString(1).substring(0, 8);
			throw new CollectException(CollectorCode.Code.NOT_FOUND_HISTORY);
		} catch(SQLException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_LOAD_HISTORY);
		} finally {
			try { rs.close(); } catch(SQLException e) {}
			try { stmt.close(); } catch(SQLException e) {}
			try { conn.close(); } catch(SQLException e) {}
		}
	}
}