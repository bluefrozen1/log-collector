package com.ksign.wizlook.bigdata.collector.collect.purge;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.util.H2ConnectionManager;
import com.ksign.wizlook.common.util.FileUtil;
/**
 * 일정기간이 지난 수집 상세 로그를 삭제하는 스케줄러 클래스
 * @author byw
 */
public enum PurgeCollectHistoryScheduler {
	INSTANCE;
	/** logger */
	private final Logger logger = LogManager.getLogger();
	/** timer for scheduler */
	private final Timer timer;
	/** history date format */
	private final SimpleDateFormat historyDf = new SimpleDateFormat("yyyyMMddHHmmss");
	/** detail log date format */
	private final SimpleDateFormat detailLogDf = new SimpleDateFormat("yyyyMMdd");

	private PurgeCollectHistoryScheduler() {
		timer = new Timer();
	}

	/**
	 * Run PurgeDetailLogScheduler
	 * 매일 00:20:00 에 동작
	 * @return
	 */
	public boolean start() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 20);
		cal.set(Calendar.SECOND, 0);
		try {
			timer.scheduleAtFixedRate(new PurgeDataThread(), cal.getTime(), 1000 * 60 * 60 * 24);
			return true;
		} catch (Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
		}
		return false;
	}

	/**
	 * Stop PurgeDetailLogScheduler
	 * @return 중지 결과 반환
	 */
	public boolean stop() {
		try {
			timer.cancel();
			return true;
		} catch (Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
		}
		return false;
	}

	/**
	 * Destroy PurgeDetailLogScheduler
	 * @return 중지 결과 반환
	 */
	public boolean destroy() {
		return stop();
	}


	/**
	 * 일정기간이 지난 collector의 수집이력 제거 ( collect.logging.retention.period.day 기준 )
	 *  1. H2 DB에 적재되어 있는 수집 이력 삭제
	 *    - TBL_COLLECT_HISTORY
	 *  2. 수집 상세 로그 삭제 ( 기간이 지난 디렉토리를 찾아 하위 파일까지 제거 )
	 *    - detailLog 디렉토리 하위 구조 ex) collect.logging.base.dir / logpolicyId / dataSourceId / 20140501
	 */
	class PurgeDataThread extends TimerTask {
		@Override
		public void run() {
			logger.info(" ###### Start PurgeCollectHistoryScheduler ######");

			Calendar cal = Calendar.getInstance();
			int retentionDate = Integer.parseInt(ConfigLoader.getInstance().get(Config.COLLECT_LOGGING_RETENTION_PERIOD));
			cal.add(Calendar.DATE, retentionDate * -1);

			deleteDbHistory(cal);

			deleteDetailLog(cal);
		}

		/**
		 * H2 DB에 적재되어 있는 수집 이력 삭제
		 *   - TBL_COLLECT_HISTORY
		 * @param cal 삭제 기준 날짜
		 */
		private void deleteDbHistory(Calendar cal) {
			Connection conn = null;
			Statement stmt = null;
			try {
				String checkDate = historyDf.format(cal.getTime());

				conn = H2ConnectionManager.INSTANCE.getCollectorConnection();
				stmt = conn.createStatement();
				stmt.execute("DELETE FROM TBL_COLLECT_HISTORY WHERE START_DATE <= '" + checkDate + "'");
				conn.commit();
			} catch (SQLException e) {
				logger.error(this.getClass().getSimpleName(), e);
			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
			} finally {
				if(stmt != null) try { stmt.close(); }catch(SQLException e) {}
				if(conn != null) try { conn.close(); }catch(SQLException e) {}
			}
		}

		/**
		 * 수집 상세 로그 삭제 ( 기간이 지난 디렉토리를 찾아 하위 파일까지 제거 )
		 *   - detailLog 디렉토리 하위 구조 ex) collect.logging.base.dir / logpolicyId / dataSourceId / 20140501
		 * @param cal 삭제 기준 날짜
		 */
		private void deleteDetailLog(Calendar cal) {
			logger.info(" ###### Run PurgeDetailLogScheduler ######");
			try {

				// 삭제 기준 디렉토리 명 ( 디렉토리명 날짜와 같거나 이전 디렉토리 삭제 )
				String checkDirName = detailLogDf.format(cal.getTime());

				// purge detail log
				deleteTargetFiles(ConfigLoader.getInstance().get(Config.COLLECT_LOGGING_BASE_DIR), checkDirName);

			} catch (IOException e) {
				logger.error(this.getClass().getSimpleName(), e);
			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
			}
		}

		/**
		 * 상세 로그파일 삭제
		 * 디렉토리 구조 : 베이스 디렉토리 / logpolicyId / dataSourceId / 날짜디렉토리 ex) 20140501
		 *   - 날짜디렉토리와 같거나 이전 날짜일 경우 삭제
		 * @param baseDir 베이스 디렉토리
		 * @param checkDirName 삭제할 디렉토리 날짜
		 * @throws IOException 파일 삭제 중 에러
		 */
		private void deleteTargetFiles(String baseDir, String checkDirName) throws IOException {
			// Logging Base Directory
			File loggingBaseDir = new File(baseDir);
			if(loggingBaseDir == null || !loggingBaseDir.exists()) return;

			// Logpolicy Directory
			File[] logpolicyDirList = loggingBaseDir.listFiles(new DirectoryFilter());
			if(logpolicyDirList == null || logpolicyDirList.length == 0) return;

			for(File logpolicyDir : logpolicyDirList) {
				// DataSource Directory
				File[] dataSourceDirList = logpolicyDir.listFiles(new DirectoryFilter());
				if(dataSourceDirList == null || dataSourceDirList.length == 0) return;

				for(File dataSourceDir : dataSourceDirList) {
					// Date Directory
					File[] dateDirList = dataSourceDir.listFiles(new FileNameFilter(checkDirName));
					for(File file : dateDirList) {
						logger.debug("Delete file : " + file.getAbsolutePath());
						FileUtil.deleteDir(file);
					}
				}
			}
		}

		/**
		 * 해당 파일이 디렉토리이고, 디렉토리명이 이전 날짜인지 체크하기 위한 필터
		 * name이 fileName과 같거나 작을 경우 true
		 */
		class FileNameFilter implements FilenameFilter {

			private String fileName;

			public FileNameFilter(String fileName) {
				this.fileName = fileName;
			}

			public boolean accept(File dir, String name) {
				File f = new File(dir, name);
				if (f.isDirectory()) {
					return name.compareTo(fileName) < 1;
				} else {
					return false;
				}
			}
		}

		/**
		 * 디렉토리 여부를 판단하기 위한 필터
		 */
		class DirectoryFilter implements FileFilter {
			@Override
			public boolean accept(File file) {
				if(file.isDirectory()) return true;
				return false;
			}
		}
	}
}