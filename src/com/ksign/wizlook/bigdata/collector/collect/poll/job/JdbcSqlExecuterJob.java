package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.quartz.UnableToInterruptJobException;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.IntervalUnit;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.RtMessage;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.itf.avro.CollectorInterfaceService;
import com.ksign.wizlook.bigdata.collector.itf.avro.InterfaceException;
import com.ksign.wizlook.common.util.StringUtil;

/**
 * 수집유형 JDBC 구현 클래스
 *  - JDBC Driver를 통해 DB 데이터를 수집한다.
 * 이슈
 *  - DB2 테스트 필요. DB2의 경우 jdbc의 getBytes를 사용할 경우 Exception이 발생하였던 걸로 기억함.
 *    rs.getString을 사용하였을 땐 문제가 없었지만, encoding문제 발생의 소지가 있음. 테스트 및 수정 필요.
 *  - mysql의 경우 stmt.setFetchSize(Integer.MIN_VALUE); 를 설정하지 않을 경우 메모리 문제가 발생.
 *    하여 mysql인 경우만 해당 옵션이 적용되도록 임시 처리 함 ( 타 DB는 Integer.MIN_VALUE 설정 시 에러가 발생하는 DB도 있음 )
 * @author byw
 */
public class JdbcSqlExecuterJob extends PollJob {
	/** Nio ByteBuffer Size */
	private int BUFFER_SIZE = 1000000;
	/** wizlook encoding */
	private String WIZLOOK_ENCODING = "UTF-8";
	/** Default JDBC fetch size */
	private int DEFAULT_JDBC_FETCH_SIZE = 10000;
	/** JDBC Statement */
	private Statement stmt;
	/** 쿼리가 수행중인지 여부 */
	private boolean isExecutingQuery = false;

	@Override
	protected String validateParameter(Map<String, String> jobDataMap) {

		// JDBC Driver Name
		if(Strings.isNullOrEmpty(jobDataMap.get("driver"))) return "driver";
		// JDBC 접속 url
		if(Strings.isNullOrEmpty(jobDataMap.get("url"))) return "url";
		// JDBC 접속 계정
		if(Strings.isNullOrEmpty(jobDataMap.get("user"))) return "user";
		// Query
		if(Strings.isNullOrEmpty(jobDataMap.get("query"))) return "query";

		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobDataMap) throws Exception {

		// JDBC Driver Name
		String driver = jobDataMap.get("driver");
		// JDBC 접속 url
		String url = jobDataMap.get("url");
		// JDBC 접속 계정
		String user = jobDataMap.get("user");
		// JDBC 접속 비밀번호
		String password = jobDataMap.get("password");
		// 수집 Query
		String query = jobDataMap.get("query");
		// 전처리 쿼리( 해당 쿼리의 결과가 "TRUE", "true", "1" 이 아닐경우 수집하지 않는다.
		String preCheckQuery = jobDataMap.get("preCheckQuery");
		// query timeout
		String queryTimeout = jobDataMap.get("queryTimeout");
		// Field 구분자
		String separator = jobDataMap.get("separator");
		// Set Default
		if(Strings.isNullOrEmpty(separator)) separator = "Ω";
		// Dynamic Query Info ( Json Str )
		String dynamicQueryInfo = jobDataMap.get("dynamicQueryInfo");
		String createPatternYn = jobDataMap.get("createPatternYn");

		String jobIntervalUnit = jobDataMap.get("jobIntervalUnit");
		String jobIntervalVal = jobDataMap.get("jobInterval");

		Connection conn = null;
		Statement preQueryStmt = null;
		ResultSet preQueryRs = null;
		ResultSet rs    = null;
		FileOutputStream stream = null;
		FileChannel channel = null;
		ByteBuffer buffer = null;
		String tempSaveFilePath = ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator +
				 "JDBC," + logpolicyId + "," + dataSourceId + "," + UUID.randomUUID().toString();
		File tempSaveFile = new File(tempSaveFilePath);
		int loginTimeoutSec = 0;
		int queryTimeoutSec = 0;

		try{
			// query timeout 설정
			if(!Strings.isNullOrEmpty(queryTimeout)) queryTimeoutSec = Integer.parseInt(queryTimeout);
			// login timeout 설정
			String loginTimeout = ConfigLoader.getInstance().get(Config.JDBC_LOGIN_TIMEOUT_SEC);
			if(!Strings.isNullOrEmpty(loginTimeout)) loginTimeoutSec = Integer.parseInt(loginTimeout);

			// 동적 쿼리 설정
			if(!Strings.isNullOrEmpty(dynamicQueryInfo)) {
				collectLogger.loggingCollectDetailLog("[Generate dynamic query] Before query=[" + query + "]");
				query = generateDynamicQuery(query, dynamicQueryInfo, jobIntervalUnit, jobIntervalVal);
				collectLogger.loggingCollectDetailLog("[Generate dynamic query] After query=[" + query + "]");
			}

			collectLogger.loggingCollectDetailLog("[JDBC connect] try JDBC connect. driver=[" + driver + "], url=[" + url + "]");
			Class.forName(driver);

			if(loginTimeoutSec > 0) DriverManager.setLoginTimeout( loginTimeoutSec );
			conn = DriverManager.getConnection(url, user, password);
			collectLogger.loggingCollectDetailLog("[JDBC connect] success");

			// 사전 확인 쿼리 ( 사전 확인 쿼리의 결과는 0, 1, TRUE, FALSE가 나와야 한다 )
			// 사전확인 쿼리의 결과가 0 또는 false 이면 수집은 종료된다.
			if(!Strings.isNullOrEmpty(preCheckQuery)) {
				collectLogger.loggingCollectDetailLog("[JDBC preCheckQuery] execute preCheckQuery=[" + preCheckQuery + "]");
				preQueryStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				if(queryTimeoutSec > 0) preQueryStmt.setQueryTimeout( queryTimeoutSec );
				try {
					preQueryRs = preQueryStmt.executeQuery(preCheckQuery);
				} catch(SQLException e) {
					collectLogger.loggingCollectDetailLog("[JDBC preCheckQuery] SQLException", e);
				}
				if(preQueryRs.next()) {
					boolean preQueryResult = preQueryRs.getBoolean(1);
					collectLogger.loggingCollectDetailLog("[JDBC preCheckQuery] result=[" + preQueryResult + "]");
					if(!preQueryResult) {
						return true;
					}
				} else {
					collectLogger.loggingCollectDetailLog("[JDBC preCheckQuery] is not exist query result");
					return false;
				}
			}

			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			if(queryTimeoutSec > 0) stmt.setQueryTimeout( queryTimeoutSec );

			// mysql의 경우 fatchSize를 아래와같이 설정하지 않으면 메모리가 garbage collecting 되지 않는 현상이 발생한다.
			// TODO : 타 드라이버 테스트 필요
			if("com.mysql.jdbc.Driver".equals(driver)) {
				stmt.setFetchSize(Integer.MIN_VALUE);
			} else {
				String fetchSize = ConfigLoader.getInstance().get(Config.JDBC_FETCH_SIZE);
				if(!Strings.isNullOrEmpty(fetchSize)) {
					stmt.setFetchSize(Integer.parseInt(fetchSize));
				} else {
					stmt.setFetchSize(DEFAULT_JDBC_FETCH_SIZE);
				}
			}

			if(isInterruptedJob()) return false;
			collectLogger.loggingCollectDetailLog("[JDBC query] execute query=[" + query + "]");
			long queryStartTime = System.currentTimeMillis();
			isExecutingQuery = true;
//			if(queryTimeoutSec > 0) stmt.setQueryTimeout( queryTimeoutSec );
			rs = stmt.executeQuery(query);
			isExecutingQuery = false;
			long queryElapsedTime = System.currentTimeMillis() - queryStartTime;
			collectLogger.loggingCollectDetailLog("[JDBC query] complete query. elapsed time=[" + queryElapsedTime + "ms]");
			if(isInterruptedJob()) return false;

			// 결과 column 수
			int numberOfColumn = rs.getMetaData().getColumnCount();

			byte[] separatorBytes = separator.getBytes(WIZLOOK_ENCODING);
			byte[] lineSeparatorBytes = System.lineSeparator().getBytes(WIZLOOK_ENCODING);
			byte[] bytes = null;
			stream = new FileOutputStream(tempSaveFile);
			channel = stream.getChannel();
			buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

			if("Y".equals(createPatternYn)) {
				if(!registLogpolicyPattern(rs.getMetaData(), separator)) {
					return false;
				}
			}

			collectLogger.loggingCollectDetailLog("[Save query result] Start. DB encoding=[" + (Strings.isNullOrEmpty(collectLogEncoding) ? WIZLOOK_ENCODING : collectLogEncoding) + "]");
			while(rs.next()){
				if(isInterruptedJob()) return false;
				for(int i=1; i <= numberOfColumn; i++) {
					int columnType = rs.getMetaData().getColumnType(i);
					if(Types.CHAR == columnType
							|| Types.VARCHAR == columnType
							|| Types.LONGVARCHAR == columnType
							|| Types.LONGNVARCHAR == columnType) {
						if(Strings.isNullOrEmpty(collectLogEncoding) || WIZLOOK_ENCODING.equalsIgnoreCase(collectLogEncoding)) {
							bytes = rs.getBytes(i);
						} else {
							if(rs.getBytes(i) != null) bytes = new String(rs.getBytes(i), collectLogEncoding).getBytes();
						}
					} else {
						if(rs.getBytes(i) != null) bytes = rs.getString(i).getBytes();
					}

					if(rs.wasNull()) bytes = "".getBytes(WIZLOOK_ENCODING);

					putBuffer(bytes, channel, buffer);
//
					if(i < numberOfColumn) {
						putBuffer(separatorBytes, channel, buffer);
					}
				}
				putBuffer(lineSeparatorBytes, channel, buffer);
			}
			if(buffer != null && buffer.remaining() < BUFFER_SIZE) writeBuffer(channel, buffer);
			channel.close();
			stream.close();

			if(tempSaveFile.length() > 0 && !isInterruptedJob()) {
				boolean result = false;
				long saveFileSize = super.save(tempSaveFile, "JDBC", "", true);
				if(saveFileSize > -1) result = true;
				collectLogger.loggingCollectDetailLog("[Save query result] End. File size=[" + saveFileSize + "], result=[" + ( result ? RtMessage.SUCCESS.toString() : RtMessage.FAIL.toString()) + "]");
				return result;
			} else {
				collectLogger.loggingCollectDetailLog("[Save query result] End. Not exist query result.");
			}
			return true;

		} catch(SQLException e){
			throw e;
		} catch(Exception e){
			throw e;
		} finally{
			if(channel != null) try { channel.close(); } catch (IOException e) { logger.error(this.getClass().getSimpleName(), e); };
			if(stream != null) try { stream.close(); } catch (IOException e) { logger.error(this.getClass().getSimpleName(), e); };
			File file = new File(tempSaveFilePath);
			if(file != null && file.exists()) file.delete();
			try { if(preQueryRs != null) { preQueryRs.close();   } } catch(SQLException se) { logger.error(this.getClass().getSimpleName(), se); }
			try { if(preQueryStmt != null) { preQueryStmt.close(); } } catch(SQLException se) { logger.error(this.getClass().getSimpleName(), se); }
			try { if(rs != null) { rs.close();   } } catch(SQLException se) { logger.error(this.getClass().getSimpleName(), se); }
			try { if(stmt != null) { stmt.close(); } } catch(SQLException se) { logger.error(this.getClass().getSimpleName(), se); }
			try { if(conn != null) { conn.close(); } } catch(SQLException se) { logger.error(this.getClass().getSimpleName(), se); }
		}
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		super.interrupt();
		if(isExecutingQuery) {
			try { stmt.cancel(); } catch(SQLException e) { logger.error(this.getClass().getSimpleName(), e); } 
			try { stmt.close(); } catch(SQLException e) { logger.error(this.getClass().getSimpleName(), e); }
			stmt = null;
		}
	}

	/**
	 * Buffer에 byte[]을 저장하고 Buffer가 다 차면 file에 write한다.
	 * @param bytes 저장할 byte[]
	 * @param channel 저장 파일 channel
	 * @param buffer 저장 Buffer
	 * @throws UnsupportedEncodingException
	 * @throws IOException
	 */
	private void putBuffer(byte[] bytes, FileChannel channel, ByteBuffer buffer) throws UnsupportedEncodingException, IOException {

		if( buffer.remaining() < bytes.length ) {
			writeBuffer(channel, buffer);
		}

		try {
			// buffer size보다 데이터가 큰 경우를 위한 처리
			buffer.put(bytes);
		} catch(BufferOverflowException e) {
			channel.write(ByteBuffer.wrap(bytes));
		}
	}

	/**
	 * 
	 * @param channel
	 * @param buffer
	 * @throws IOException
	 */
	private void writeBuffer(FileChannel channel, ByteBuffer buffer) throws IOException {
		buffer.flip();
		channel.write(buffer);
		buffer.clear();
	}

	/**
	 * 동적쿼리일 경우 쿼리를 생성
	 * @param query 사용자 입력 쿼리
	 * @param dynamicQueryInfo 동적쿼리 정보 ( JSON String )
	 * @param jobIntervalUnit 동작 주기 단위
	 * @param jobIntervalVal 동작 주기 값
	 * @return 동적 쿼리
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 */
	private String generateDynamicQuery(String query, String dynamicQueryInfo, String jobIntervalUnit, String jobIntervalVal) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		List<Map<String, String>> dynamicInfoList = mapper.readValue(dynamicQueryInfo, new TypeReference<List<Map<String, String>>>() {});
		if(dynamicInfoList != null) {
			String parsedQuery = query;
			boolean isUnionQuery = false;
			Map<String, String> unionInfoMap = new HashMap<String, String>();
			for(Map<String, String> dynamicInfo : dynamicInfoList) {
				SimpleDateFormat df = new SimpleDateFormat(dynamicInfo.get("format"));
				String dateOffset = dynamicInfo.get("dateOffset");
				String dateOffsetFactor = dynamicInfo.get("dateOffsetFactor");
				String offsetDateStr = null;
				Date offsetDate = null;
				if(!Strings.isNullOrEmpty(dateOffset) && !Strings.isNullOrEmpty(dateOffsetFactor)) {
					offsetDate = getBeforeTableDate(this.fireTime, dateOffset, dateOffsetFactor);
				} else {
					offsetDate = this.fireTime;
				}
				offsetDateStr = df.format(offsetDate);
				String partitionUnit = dynamicInfo.get("partitionUnit");
				String partitionFactor = dynamicInfo.get("partitionFactor");
				parsedQuery = parsedQuery.replace(dynamicInfo.get("key"), offsetDateStr);
		 
				// partitionTable 일 경우
				// 이전 partition 테이블과 UNION 을 할 지 여부 체크
				if(!Strings.isNullOrEmpty(partitionUnit) && !Strings.isNullOrEmpty(partitionFactor)) {
					Date checkDate = getBeforeTableDate(offsetDate, jobIntervalUnit, jobIntervalVal);

					String checkDateStr = df.format(checkDate);
					if(offsetDateStr.compareTo(checkDateStr) > 0) {
						isUnionQuery = true;
						Date beforeTableDate = getBeforeTableDate(offsetDate, partitionUnit, partitionFactor);
						unionInfoMap.put(dynamicInfo.get("key"), df.format(beforeTableDate));
					} else {
						unionInfoMap.put(dynamicInfo.get("key"), offsetDateStr);
					}
				} else {
					unionInfoMap.put(dynamicInfo.get("key"), offsetDateStr);
				}
			}
			if(isUnionQuery) {
				String unionQuery = query;
				for(Map.Entry<String, String> map : unionInfoMap.entrySet()) {
					unionQuery = unionQuery.replace(map.getKey(), map.getValue());
				}
				parsedQuery = "SELECT * FROM ( " + unionQuery + ") a UNION SELECT * FROM ( " + parsedQuery + ") b";
			}
			return parsedQuery;
		}
		return query;
	}

	/**
	 * 실행 날짜를 기반으로 인터벌 단위, 인터벌 값을 통해 과거 날짜를 계산한다.
	 * @param executeDate 실행 날짜
	 * @param unit 인터벌 단위
	 * @param valueStr 인터벌 값
	 * @return 계산된 날짜
	 * @throws NumberFormatException
	 */
	private Date getBeforeTableDate(Date executeDate, String unit, String valueStr) throws NumberFormatException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(executeDate);

		int value = Integer.parseInt(valueStr);
		if(IntervalUnit.SECOND.toString().equals(unit)) cal.add(Calendar.SECOND, -1 * value);
		else if(IntervalUnit.MINUTE.toString().equals(unit)) cal.add(Calendar.MINUTE, -1 * value);
		else if(IntervalUnit.HOUR.toString().equals(unit)) cal.add(Calendar.HOUR_OF_DAY, -1 * value);
		else if(IntervalUnit.DAY.toString().equals(unit)) cal.add(Calendar.DAY_OF_MONTH, -1 * value);
		else if(IntervalUnit.MONTH.toString().equals(unit)) cal.add(Calendar.MONTH, -1 * value);
		else if(IntervalUnit.YEAR.toString().equals(unit)) cal.add(Calendar.YEAR, -1 * value);
		return cal.getTime();
	}

	/**
	 * 정책 패턴을 생성하고 console에 등록 요청
	 * @param rsmd ResultSetMetaData
	 * @param separator 구분자
	 * @throws SQLException 
	 */
	private boolean registLogpolicyPattern(ResultSetMetaData rsmd, String separator) throws Exception {
		collectLogger.loggingCollectDetailLog("[Logpolicy pattern] Auto create. PatternId=[" + logpolicyId + "_PT]");
		try {
			Pattern pattern = Pattern.compile("[a-zA-Z]+[a-zA-Z0-9]*");
			Map<String, String> patternMap = new LinkedHashMap<String, String>();
			for(int i = 1; i <= rsmd.getColumnCount(); i++) {
				String groupCaptureName = StringUtil.getDbColumnToJavaVariable(rsmd.getColumnLabel(i).toLowerCase());
				if(patternMap.put(groupCaptureName, groupCaptureName) != null) {
					collectLogger.loggingCollectDetailLog("[Logpolicy pattern] ERROR. Is not allow duplicate column label. column label=[" + groupCaptureName + "]");
					return false;
				}
				Matcher matcher = pattern.matcher(groupCaptureName);
				if(!matcher.matches()) {
					String msg = "[Logpolicy pattern] ERROR. Is invalid column label.\n"
							+ " Column label must be composed of the following characters.\n"
							+ "   - The uppercase letters 'A' through 'Z' \n" 
							+ "   - The lowercase letters 'a' through 'z' \n"
							+ "   - The digits '0' through '9' \n"
							+ " AND The first character must be a letter.";
					collectLogger.loggingCollectDetailLog(msg);
					return false;
				}
			}
			StringBuilder logpolicyPattern = new StringBuilder();
			boolean isFirst = true;
			for(String columnLabel : patternMap.values()) {
				if(isFirst) isFirst = false;
				else logpolicyPattern.append(separator);
				logpolicyPattern.append("(?<").append(columnLabel).append(">[^").append(separator).append("]*)");
			}
			CollectorInterfaceService.INSTANCE.registLogpolicyPattern(logpolicyId, logpolicyId + "_PT", logpolicyPattern.toString());
		} catch (InterfaceException e) {
			collectLogger.loggingCollectDetailLog("[Logpolicy pattern] Error.", e);
			return false;
		} catch (SQLException e) {
			collectLogger.loggingCollectDetailLog("[Logpolicy pattern] Error.", e);
			return false;
		} catch (Exception e) {
			collectLogger.loggingCollectDetailLog("[Logpolicy pattern] Error.", e);
			return false;
		}
		collectLogger.loggingCollectDetailLog("[Logpolicy pattern] Auto create SUCCESS.");
		return true;
	}
}