package com.ksign.wizlook.bigdata.collector.collect.push;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * PushReceier의 bufferSave를 통해 수집된 로그파일을 저장할 경우
 * 새로운 로그가 수집되지 않으면 파일이 롤링되지 않기 때문에
 * 이를 해결하기 위해 롤링 시간이 지난 파일을 강제적으로 롤링 처리 해주는 Thread 클래스
 * @author byw
 *
 */
public class PushReceiveLogRollingThread implements Runnable {

	private long interval = 0;
	private Logger logger = LogManager.getLogger();

	public PushReceiveLogRollingThread(long interval) {
		this.interval = interval;
	}

	/**
	 * 추가적인 로그가 들어오지 않으면 File이 롤링되지 않음
	 * 그래서 interval이 지난 File들을 강제 롤링
	 */
	@Override
	public void run() {
		logger.info("	Start " + this.getClass().getSimpleName());
		while(!Thread.currentThread().isInterrupted()) {

			try {
				PushReceiverManager.INSTANCE.rollingReceiveLogFile();
			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
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