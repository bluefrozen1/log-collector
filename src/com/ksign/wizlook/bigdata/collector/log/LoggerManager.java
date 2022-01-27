package com.ksign.wizlook.bigdata.collector.log;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;

import com.ksign.wizlook.bigdata.collector.log.FileRollingLogger.RollingIntervalUnit;

public class LoggerManager {
	/** Singleton instance */
	private static volatile LoggerManager instance;
	/** 생성된 로거를 담고 있는 Map */
	private Map<String, FileRollingLogger> loggerMap;
	/** 파일 강제 롤링 Thread */
	private Thread fileForceRollingThread;

	public static synchronized LoggerManager getInstance() {
		if(instance == null) {
			instance = new LoggerManager();
		}
		return instance;
	}

	private LoggerManager() {
		loggerMap = new HashMap<String, FileRollingLogger>();
		fileForceRollingThread = new Thread(new FileForceRollingThread());
		fileForceRollingThread.start();
	}

	/**
	 * File Rolling Logger를 생성하고 LoggerManager에 등록한다.
	 * @param loggerName 로거명
	 * @param appendFilePath 로깅파일
	 * @param rollingFilePath 롤링파일 ( 파일명에 _%d{SimpleDateFormat} 형태의 문자가 들어가면 해당 부분이 현재 날짜로 치환된다.
	 * 						  ex) test_%d{yyyyMMdd}.log => test_20160705.log
	 * @param rollingIntervalUnit 롤링주기단위
	 * @param rollingInterval 롤링주기
	 * @throws LoggerException
	 */
	public void addFileRollingLogger(String loggerName, String appendFilePath, String rollingFilePath, RollingIntervalUnit rollingIntervalUnit, long rollingInterval) throws LoggerException {
		FileRollingLogger fileRollingLogger = new FileRollingLogger(appendFilePath, rollingFilePath, rollingIntervalUnit, rollingInterval);
		loggerMap.put(loggerName, fileRollingLogger);
	}

	/**
	 * Logger를 제거한다.
	 * @param loggerName 로거명
	 */
	public void removeFileRollingLogger(String loggerName) {
		loggerMap.remove(loggerName);
	}

	/**
	 * Logger를 반환한다.
	 * @param loggerName 로거명
	 * @return
	 */
	public FileRollingLogger getFileRollingLogger(String loggerName) {
		return loggerMap.get(loggerName);
	}

	public void destroy() {
		if(fileForceRollingThread != null) {
			try { fileForceRollingThread.interrupt(); } catch(Exception e) {}
		}
	}
	/**
	 * FileRollingLogger는 새로운 로그가 발생하였을때 롤링 주기를 체크하여 rename을 수행하도록 구현되어 있다.
	 * 이 Thread는 주기적으로 로그파일을 체크하여 신규 로그가 발생하지 않아도 강제로 파일을 롤링하는 작업을 수행한다.
	 * @author byw
	 */
	class FileForceRollingThread implements Runnable {
		@Override
		public void run() {
			LogManager.getLogger().info("Start FileForceRollingThread in LoggerManager");
			while(!Thread.currentThread().isInterrupted()) {

				try {
					for(FileRollingLogger rollingLogger : loggerMap.values()) {
						rollingLogger.rollingFile();
					}
				} catch (Exception e) {
					LogManager.getLogger().error(this.getClass().getSimpleName(), e);
				}

				try {
					Thread.sleep(1000L); 
				} catch (InterruptedException e) {
					break;
				}
			}
			LogManager.getLogger().info("Stop FileForceRollingThread in LoggerManager");
		}
	}

//	public static void addLogger(String id, String logPath, String rollingLogPath, String rollingInterval, String logEncoding) {
//		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
//
//		PatternLayout layout = PatternLayout.createLayout("%m%n", null, null, null, Charset.forName(logEncoding), false, false, null, null);
//		Configuration config = ctx.getConfiguration();
//
//		Appender appender = RollingFileAppender.createAppender(logPath, rollingLogPath, "true", id+"_appender", "true", "", "true", TimeBasedTriggeringPolicy.createPolicy(rollingInterval, "true"), null, layout, null, "", "", "", config);
//		appender.start();
//
//		AppenderRef ref = AppenderRef.createAppenderRef(id, Level.DEBUG, null);
//		AppenderRef[] refs = new AppenderRef[] {ref};
//
//		LoggerConfig loggerConfig = LoggerConfig.createLogger("true", Level.DEBUG, id, "false", refs, null, config, null);
//		loggerConfig.addAppender(appender, Level.DEBUG, null);
//		config.addLogger(id, loggerConfig);
//		ctx.updateLoggers(config);
//	}
//	
//	public static void removeLogger(String id) {
//		LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
//		Map<String, LoggerConfig> loggerMap = ctx.getConfiguration().getLoggers();
//
//		Map<String, Appender> appenderMap = loggerMap.get(id).getAppenders();
//		for(Appender appender : appenderMap.values()) {
//			appender.stop();
//		}
//		ctx.getConfiguration().removeLogger(id);
//	}
//	
//	
//	public static void main(String[] args) {
//		LoggerManager.addLogger("byw_test", "D:/log4j/log4j", "D:/log4j/log4j_%d{yyyyMMddHHmmss}.log", "4", "UTF-8");
//
//		try {
//			int i=0;
//			while(true) {
//				LogManager.getLogger("byw_test").info("test" + i);
//				try {
//					Thread.sleep(1000);
//					i++;
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				if(i==10) {
//					removeLogger("byw_test");
//					break;
//				}
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//			
//		try {
//			Thread.sleep(100000000L);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//	}
}
