package com.ksign.wizlook.bigdata.collector.collect.history;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Serie;
import org.influxdb.dto.Serie.Builder;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.AbstractCollect;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * File에 저장되어 있는 Dashboard용 로그를 Influx DB에 Insert하기 위한 Thread 클래스
 * @author byw
 */
public class CollectHistoryDashboardInsertThread implements Runnable {
	/** collectHistoryLogger append file name */
	private final String appendFileName;
	/** collectHistoryLogger rolling directory */
	private final File rollingDir;
	/** log queue for bulk insert */
	private final BlockingQueue<String> queue;
	/** log queue size */
	private final int queueSize;
	/** log queue bulk insert thread */
	private Thread insertThread;
	/** root logger */
	private Logger logger = LogManager.getLogger();

	public CollectHistoryDashboardInsertThread() throws RuntimeException, ConnectException, IOException {
		// log4j2 설정에서 감사로그 저장 파일 및 롤링 디렉토리 조회
		LoggerContext loggerContext = (LoggerContext)LogManager.getContext(false);
		Appender auditRollingAppender = loggerContext.getConfiguration().getAppender("collectHistoryDashboardRollingFile");
		if(auditRollingAppender == null || !(auditRollingAppender instanceof RollingFileAppender)) {
			throw new RuntimeException("Not found 'collectHistoryDashboardLogger' in log4j2 configuration");
		}
		RollingFileAppender rollingAppender = (RollingFileAppender)auditRollingAppender;
		String rollingFilePattern = rollingAppender.getFilePattern();
		this.appendFileName = new File(rollingAppender.getFileName()).getName();
		this.rollingDir = new File(rollingFilePattern).getParentFile();
		this.queueSize = ConfigLoader.getInstance().getInt(Config.LOG_DB_BULK_INSERT_SIZE);
		this.queue = new ArrayBlockingQueue<String>(queueSize);
		this.insertThread = new Thread(new InsertThread());
		insertThread.start();
	}

	@Override
	public void run() {
		// 1. 수집로그가 저장된 디렉토리 스캔
		// 2. 스캔된 파일을 읽어 내용을 queue에 적재
		// 3. 파일 삭제
		logger.info("	Start " + this.getClass().getSimpleName());
		while(!Thread.currentThread().isInterrupted()) {
			File[] fileList = rollingDir.listFiles(new FileFilter(){
				@Override
				public boolean accept(File file) {
					if(!file.isFile() || file.getName().equalsIgnoreCase(appendFileName)) return false;
					else return true;
				}
			});

			if(fileList == null || fileList.length == 0) {
				try { Thread.sleep(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
				continue;
			} else {
				// log4j가 rolling중일 수도 있으니 10ms만 기다렸다가 file을 open한다.
				try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			}

			for(File file : fileList) {
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(file));
					String value = null;
					while((value = reader.readLine()) != null) {
						if(Strings.isNullOrEmpty(value)) continue;
						queue.put(value);
					}
					reader.close();
					file.delete();
				} catch (InterruptedException e) {
					logger.error(this.getClass().getSimpleName(), e);
					Thread.currentThread().interrupt();
				} catch (IOException e) {
					logger.error(this.getClass().getSimpleName(), e);
				} finally {
					if(reader != null) try { reader.close(); } catch (IOException e) {} 
				}
			}
		}
		logger.info("	Stop " + this.getClass().getSimpleName());
		insertThread.interrupt();
	}

	class InsertThread implements Runnable {
		/** influx db client */
		private InfluxDB influxDB;
		/** influx bulk insert size */
		private final int bulkSize = 200;
		/** influx db name */
		private final String dbName;
		/** influx db audit history table name */
		private final String collectHistoryTableName;

		public InsertThread() throws IOException {
			// influxDB client 생성
			ConfigLoader config = ConfigLoader.getInstance();

			influxDB = InfluxDBFactory.connect(config.get(Config.LOG_DB_URL),
											   config.get(Config.LOG_DB_USERNAME),
											   config.get(Config.LOG_DB_PASSWORD));

			this.dbName = config.get(Config.LOG_DB_NAME);
			this.collectHistoryTableName = config.get(Config.LOG_DB_TABLE_NAME);

			try { influxDB.createDatabase(dbName); } catch(RuntimeException e) { }
		}

		@Override
		public void run() {
			// 1. 현재 queueSize가 0 이면 1초 sleep
			// 2. queue에 데이터가 있다면 bulk처리 ( 최대 bulkSize만큼 합쳐서 influx serie 생성 )
			// 3.  insert influx DB
			while(!Thread.currentThread().isInterrupted()) {
				if(queue.size() == 0) {
					try {
						Thread.sleep(1000);
						continue;
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}

				Builder builder = new Serie.Builder(collectHistoryTableName);
				builder.columns("time", "logpolicyId", "dataSourceId", "fileName", "fileSize");

				int pollCount = 0;
				while(pollCount <= bulkSize) {
					String value = queue.poll();
					if(value == null) break;
					String[] valueArr = value.split(AbstractCollect.COLLECTOR_SEPARATOR, 5);
					long time = Long.parseLong(valueArr[0]);
					String logpolicyId = valueArr[1];
					String dataSourceId = valueArr[2];
					String fileName = valueArr[3];
					long fileSize = Long.parseLong(valueArr[4]);
					builder.values(time, logpolicyId, dataSourceId, fileName, fileSize);
					pollCount++;
				}

				int tryCnt = 0;
				while(tryCnt < 4) {
					tryCnt++;
					try {
						if(pollCount > 0) influxDB.write(dbName, TimeUnit.MILLISECONDS, builder.build());
						break;
					} catch (Exception e) {
						logger.error(this.getClass().getSimpleName(), e);
					}
				}
			}
		}
	}
}