package com.ksign.wizlook.bigdata.collector.log;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;

public class Log4j2ForceRollingThread implements Runnable {

	private long interval = 0;
	private Logger logger = LogManager.getLogger();

	public Log4j2ForceRollingThread(long interval) {
		this.interval = interval;
	}

	/**
	 * Log4j2 특성상 TimeBaseRollingFileAppender를 사용하여도 추가적인 로그가 들어오지 않으면 File이 롤링되지 않음
	 * 그래서 TimeBaseRollingPolicy를 조회하여 Rolling interval이 지난 File들을 강제 롤링
	 */
	@Override
	public void run() {
		logger.info("	Start " + this.getClass().getSimpleName());
		while(!Thread.currentThread().isInterrupted()) {

			LoggerContext loggerContext = (LoggerContext)LogManager.getContext(false);

			Map<String, Appender> appenderMap = loggerContext.getConfiguration().getAppenders();

			for(Map.Entry<String, Appender> entry : appenderMap.entrySet()) {
				Appender appender = entry.getValue();

				// RollingFileAppender 일 경우만
				if(appender instanceof RollingFileAppender) {
					RollingFileAppender rollingAppender = (RollingFileAppender)appender;

					// TimeBaseRolling 일 경우만
					if(rollingAppender.getManager().getTriggeringPolicy() instanceof TimeBasedTriggeringPolicy) {
						LogEvent logEvent = new Log4jLogEvent();

						// Rolling시간이 지났는데 File이 롤링되지 않을 경우만 즉시 롤링이 호출됨(checkRollover)
						rollingAppender.getManager().checkRollover(logEvent);
					}
				}
			}
			try { 
				Thread.sleep(interval); 
			} catch (InterruptedException e) {
				logger.error(this.getClass().getSimpleName(), e);
				Thread.currentThread().interrupt();
			}
		}
		logger.info("	Stop " + this.getClass().getSimpleName());
	}
}