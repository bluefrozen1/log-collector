package com.ksign.wizlook.bigdata.collector.log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;

import com.google.common.base.Strings;

/**
 * log4j2의 fileRollingLogger 컨셉을 따 직접 구현한 클래스
 * log4j2는 Collector가 Shutdown될 때 logging을 하지 못하는 이슈가 있기 때문에, 
 * 수집 중지 이력을 남기지 못해 이 로거를 직접 구현하여 사용
 * @author byw
 */
public class FileRollingLogger {
	/** Lock Object */
	private Object writeLock = new Object();
	/** 기본적으로 로그가 쓰여지는 파일 */
	private File appendFile;
	/** 기본적으로 로그가 쓰여지는 파일 경로 */
	private String appendFilePath;
	/** 파일 롤링 경로 */
	private String rollingFilePath;
	/** 파일 롤링 주기(ms) */
	private long fileRollingIntervalMillis;
	/** 파일 생성 시간 */
	private long fileCreateTime;

	/** File Rolling Interval Unit */
	public static enum RollingIntervalUnit {
		SECOND,
		MINUTE,
		HOUR,
		DAY;
	}

	/**
	 * FileRollingLogger 생성
	 * @param appendFilePath 기본적으로 로깅할 파일 경로
	 * @param rollingFilePath 롤링할 파일 패턴 및 경로 ( ex) test_%d{yyyyMMdd}.log )
	 * @param rollingIntervalUnit 롤링 주기 단위
	 * @param rollingInterval 롤링 인터벌
	 * @throws LoggerException
	 */
	public FileRollingLogger(String appendFilePath, String rollingFilePath, RollingIntervalUnit rollingIntervalUnit, long rollingInterval) throws LoggerException {
		this.appendFilePath = appendFilePath;
		this.rollingFilePath = rollingFilePath;
		this.fileCreateTime = System.currentTimeMillis();
		this.rollingFilePath = rollingFilePath;

		if(Strings.isNullOrEmpty(appendFilePath)) throw new LoggerException("appendFilePath cannot be null or empty");
		this.appendFile = new File(appendFilePath);
		if(!appendFile.getParentFile().exists()) {
			appendFile.getParentFile().mkdirs();
		}

		if(Strings.isNullOrEmpty(rollingFilePath)) throw new LoggerException("rollingFilePath cannot be null or empty");
		File rollingFile = new File(rollingFilePath);
		if(!rollingFile.getParentFile().exists()) {
			rollingFile.getParentFile().mkdirs();
		}

		if(rollingInterval < 1) throw new LoggerException("rollingInterval must be over zero.");

		if(RollingIntervalUnit.SECOND == rollingIntervalUnit) {
			this.fileRollingIntervalMillis = rollingInterval * 1000L; 
		} else if(RollingIntervalUnit.MINUTE == rollingIntervalUnit) {
			this.fileRollingIntervalMillis = rollingInterval * 1000L * 60L;
		}  else if(RollingIntervalUnit.HOUR == rollingIntervalUnit) {
			this.fileRollingIntervalMillis = rollingInterval * 1000L * 60L * 60L;
		} else if(RollingIntervalUnit.DAY == rollingIntervalUnit) {
			this.fileRollingIntervalMillis = rollingInterval * 1000L * 60L * 60L * 24L;
		} else {
			throw new LoggerException("Is invalid RollingIntervalUnit. RollingIntervalUnit=[" + rollingIntervalUnit + "]");
		}
	}

	/**
	 * 기본 append file에 로그를 기록한다.
	 * 로그 기록 후 파일의 생성시간과 롤링인터벌을 비교하여 파일을 롤링한다.
	 * @param log
	 */
	public void write(String log) {
		synchronized(writeLock) {
			FileWriter writer = null;
			try {
				if(!appendFile.exists()) fileCreateTime = System.currentTimeMillis();
				writer = new FileWriter(appendFile, true);
				writer.write(log);
				writer.write(System.lineSeparator());
				writer.flush();
			} catch (IOException e) {
				LogManager.getLogger().error(this.getClass().getSimpleName(), e);
			} finally {
				try { writer.close(); } catch (IOException e) {}
			}

			long currentTime = System.currentTimeMillis();
			if((currentTime - fileCreateTime) >= fileRollingIntervalMillis) {
				appendFile.renameTo(new File(getRollingFileName(currentTime)));
			}
		}
	}

	/**
	 * 롤링하기 위한 파일명을 가져온다.
	 * 파일명에 d%{ SimpleDateFormat } 형태의 데이터가 들어있으면 해당 부분을 현재 시간으로 치환한다.
	 * ex) test_%d{yyyyMMdd}.log => test_20160705.log
	 * @param currentTime 현재 시간
	 * @return
	 */
	private String getRollingFileName(long currentTime) {
		Matcher matcher = Pattern.compile("%d\\{(?<dateFormat>\\w+)\\}").matcher(this.rollingFilePath);
		if(matcher.find()) {
			matcher.start();
			matcher.end();
			try {
				SimpleDateFormat df = new SimpleDateFormat(matcher.group(1));
				String convertedRollingFilePath = rollingFilePath.substring(0, matcher.start()) + df.format(new Date(currentTime)) + rollingFilePath.substring(matcher.end(), rollingFilePath.length());
				return convertedRollingFilePath;
			} catch(IllegalArgumentException e) {
				LogManager.getLogger().error(this.getClass().getSimpleName(), e);
			}
		}
		return this.rollingFilePath;
	}

	/**
	 * 파일 생성시간과 파일 롤링인터벌을 비교하여 파일을 롤링한다.
	 * @throws IOException
	 * @throws ParseException
	 */
	public void rollingFile() throws IOException, ParseException {
		long currentTime = System.currentTimeMillis();
		if((currentTime - fileCreateTime) >= fileRollingIntervalMillis) {
			synchronized(writeLock) {
				appendFile.renameTo(new File(getRollingFileName(currentTime)));
			}
		}
	}

	public String getAppendFilePath() {
		return appendFilePath;
	}

	public String getRollingFilePath() {
		return rollingFilePath;
	}
}