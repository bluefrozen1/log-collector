package com.ksign.wizlook.bigdata.collector.collect.history;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.collect.AbstractCollect;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.log.LoggerManager;

/**
 * 수집시작, 수집종료, 수집상세 정보를 로깅하는 클래스 
 * 한번의 수집 파일 단위로 이 클래스를 생성해야되며 메소드는 아래와 같은 순서로 호출되어야 한다.
 * 	- loggingCollectStart ( 로그 수집 시작 시 호출 )
 * 	- loggingCollectDetailLog ( 수집 상세 로그 기록이 필요할때마다 호출 )
 * 	- loggingCollectEnd ( 로그 수집 완료 시 호출 )
 * 여기서 생성된 이력은 log4j2가 아닌 직접 구현한 Logger를 통해 남기며 (log4j2는 shutdown시 log를 남길 수 없기 때문)  
 * CollectHistoryInsertThread 가 이 파일을 H2 이력 DB에 Insert한다.
 */
public class CollectLogger {

	/** 수집 이력 시작 */
	public static final String COLLECT_LOGGING_START = "S";
	/** 수집 이력 종료 */
	public static final String COLLECT_LOGGING_END = "E";

	/** 로그 날짜 포맷 */
	private final SimpleDateFormat logDf = new SimpleDateFormat("yyyyMMddHHmmss");
	/** 상세로그 날짜 포맷 */
	private SimpleDateFormat detailLogDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private SimpleDateFormat detailLogFileNameDf = new SimpleDateFormat("yyyyMMdd");
	/** 상세로그 Writer lock */
	private Object writerLock = new Object();
	/** root logger */
	private final Logger rootLogger = LogManager.getLogger();
	/** 정책아이디 */
	private String logpolicyId;
	/** 데이터소스아이디 */
	private String dataSourceId;
	/** 수집 세션아이디 */
	private String sessionId;
	/** 수집 시작 시간 */
	private long startDate;

	public CollectLogger(String logpolicyId, String dataSourceId) {
		this.logpolicyId = logpolicyId;
		this.dataSourceId = dataSourceId;
		this.sessionId = UUID.randomUUID().toString();
	}

	/**
	 * 수집 상세 이력 기록
	 * @param jobDetailLog
	 */
	public void loggingCollectDetailLog(String jobDetailLog) {
		loggingCollectDetailLog(jobDetailLog, null);
	}

	/**
	 * 수집 상세 이력 기록
	 * 상세이력파일 생성 위치 : collect.logging.base.dir/logpolicyId/dataSourceId/
	 * @param jobDetailLog
	 * @param t
	 */
	public void loggingCollectDetailLog(String jobDetailLog, Throwable t) {
		if(!ConfigLoader.getInstance().getBoolean(Config.COLLECT_LOGGING_ENABLED)) return;
		synchronized(writerLock) {
			File loggingFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_LOGGING_BASE_DIR) + 
										File.separator + logpolicyId + 
										File.separator + dataSourceId +
										File.separator + detailLogFileNameDf.format(new Date(startDate)) + 
										File.separator + sessionId + ".log");
			if(!loggingFile.getParentFile().exists()) loggingFile.getParentFile().mkdirs();
			FileWriter detailLogWriter = null;
			try {
				detailLogWriter = new FileWriter(loggingFile, true);

				StringBuilder log = new StringBuilder();
				log.append("[").append(detailLogDf.format(new Date())).append("] ").append(jobDetailLog);
				if(t != null) {
					log.append(System.lineSeparator()).append(t.toString()).append(System.lineSeparator());
					for(StackTraceElement element : t.getStackTrace()) {
						log.append("\t").append(element.toString()).append(System.lineSeparator());
					}
				}
				log.append(System.lineSeparator());
				detailLogWriter.write(log.toString());
				detailLogWriter.flush();
			} catch (IOException e) {
				rootLogger.error(this.getClass().getSimpleName(), e);
			} finally {
				if(detailLogWriter != null) try { detailLogWriter.close(); } catch(IOException e) {}
			}
		}
	}

	/**
	 * 수집 시작 로깅
	 * @param startDate 시작 일자
	 */
	public void loggingCollectStart(long startDate) {
		if(!ConfigLoader.getInstance().getBoolean(Config.COLLECT_LOGGING_ENABLED)) return;
		this.startDate = startDate;
		StringBuilder collectHistoryLog = new StringBuilder().append(COLLECT_LOGGING_START)
			   		  								  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(sessionId)
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(logDf.format(new Date(startDate)))
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append("")
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(logpolicyId)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(dataSourceId)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(CollectStatus.RUNNING)
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append("")
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append("");
		LoggerManager.getInstance().getFileRollingLogger("collectHistoryLogger").write(collectHistoryLog.toString());
	}

	/**
	 * 수집 종료 로깅
	 * @param endDate 종료 일자
	 * @param collectFileSize 수집 파일 크기
	 * @param status 수집결과 SUCCESS, KILLED, ERROR
	 */
	public void loggingCollectEnd(long endDate, long collectFileSize, CollectStatus status) {
		if(!ConfigLoader.getInstance().getBoolean(Config.COLLECT_LOGGING_ENABLED)) return;
		StringBuilder collectHistoryLog = new StringBuilder().append(COLLECT_LOGGING_END)
			   		  								  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(sessionId)
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(logDf.format(new Date(startDate)))
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(logDf.format(new Date(endDate)))
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(logpolicyId)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(dataSourceId)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(status == null ? "" : status.toString())
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(endDate - startDate)
													  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(collectFileSize);
		LoggerManager.getInstance().getFileRollingLogger("collectHistoryLogger").write(collectHistoryLog.toString());
	}
}