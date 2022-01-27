package com.ksign.wizlook.bigdata.collector.collect.purge;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.common.util.FileUtil;

/**
 * 일정기간이 지난 Backup 로그를 주기적으로 삭제하는 스케줄러 클래스
 * @author byw
 */
public enum PurgeBackupDataScheduler {
	INSTANCE;
	/** logger */
	private final Logger logger = LogManager.getLogger();
	private final Timer timer;
	private final SimpleDateFormat sd = new SimpleDateFormat("yyyyMMdd");

	private PurgeBackupDataScheduler() {
		timer = new Timer();
	}

	/**
	 * Run PurgeDataScheduler
	 * 매일 00:10:00 에 동작
	 * @return
	 */
	public boolean start() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 10);
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
	 * Stop PurgeDataScheduler
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
	 * Destroy PurgeDataScheduler
	 * @return 중지 결과 반환
	 */
	public boolean destroy() {
		return stop();
	}


	/**
	 * 1. DB에서 retention이 Y인 logpolicy 조회
	 * 2. collector의 backup 디렉토리에서 retention 기간이 지난 디렉토리를 찾아 하위 파일까지 제거
	 *    backup 디렉토리 하위 구조 ex) logpolicyId / dataSourceId / 20140501
	 * @author byw
	 *
	 */
	class PurgeDataThread extends TimerTask {
		@Override
		public void run() {
			logger.info(" ###### Start PurgeBackupDataScheduler ######");
			try {
				Calendar cal = Calendar.getInstance();
				int retentionDate = Integer.parseInt(ConfigLoader.getInstance().get(Config.BACKUP_LOG_RETENTION_PERIOD));
				cal.add(Calendar.DATE, retentionDate * -1);

				String checkDirName = sd.format(cal.getTime());

				// purge backupLog
				deleteTargetFiles(ConfigLoader.getInstance().get(Config.BACKUP_LOG_BASE_DIR), checkDirName);

				// purge errorLog
				deleteTargetFiles(ConfigLoader.getInstance().get(Config.COLLECT_ERROR_BASE_DIR), checkDirName);

			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
			}
		}

		/**
		 * 저장 디렉토리 구조에 맞게 하위 파일 삭제
		 * backup 디렉토리 하위 구조 ex) logpolicyId / dataSourceId / 20140501
		 * @param baseDir Backup base 디렉토리
		 * @param checkDirName 삭제 기준 날자
		 * @throws IOException
		 */
		private void deleteTargetFiles(String baseDir, String checkDirName) {
			
			try {
				
				// Backup Base Directory
				File backupBaseDir = new File(baseDir);
				if(backupBaseDir == null || !backupBaseDir.exists()) return;

				// Logpolicy Directory
				File[] logpolicyDirList = backupBaseDir.listFiles(new DirectoryFilter());
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
			} catch (IOException e) {
				logger.error(this.getClass().getSimpleName(), e);
			}
		}

		/**
		 * FileNameFilter
		 * 해당 파일이 디렉토리이고, 디렉토리명이 이전 날짜인지 체크하기 위한 필터
		 * @author byw
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

		class DirectoryFilter implements FileFilter {
			@Override
			public boolean accept(File file) {
				if(file.isDirectory()) return true;
				return false;
			}
		}
	}
}