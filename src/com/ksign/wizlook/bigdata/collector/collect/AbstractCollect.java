package com.ksign.wizlook.bigdata.collector.collect;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3FileHandle;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * Polling 수집 시 Dashboard 로깅 및 수집로그 저장이 구현된 추상 클래스
 * @author byw
 */
public abstract class AbstractCollect {
	/** root logger */
	protected final Logger logger = LogManager.getLogger();
	/** 정책아이디 */
	protected String logpolicyId;
	/** 데이터소스아이디 */
	protected String dataSourceId;
	/** 버퍼크기 */
	private int bufferSize = 4096;

	/** 콜렉터가 내부적으로 사용하는 구분자 */
	public static final String COLLECTOR_SEPARATOR = "α";
	/** 로그 날짜 포맷 */
	private final SimpleDateFormat logDf = new SimpleDateFormat("yyyyMMddHHmmss");

	public AbstractCollect() {
	}

	public void init(String logpolicyId, String dataSourceId) {
		this.logpolicyId = logpolicyId;
		this.dataSourceId = dataSourceId;
	}

	/**
	 * 수집 모듈 시작
	 * @return
	 * @throws Exception
	 */
	public abstract boolean start() throws Exception;

	/**
	 * 수집 모듈 중지
	 * @return
	 * @throws Exception
	 */
	public abstract boolean stop() throws Exception;

	/**
	 * 수집데이터 저장
	 * @param byteData 수집한 byte[] 데이터
	 * @param fileName 저장 파일명
	 * @param logEncoding 저장로그 인코딩
	 * @param append append 여부
	 * @return 수집로그 크기
	 * @throws CollectException
	 */
	public long save(byte[] byteData, String fileName, String logEncoding, boolean append) throws CollectException {
		return this.writeToFile(byteData, fileName, logEncoding, append);
	}

	/**
	 * 수집데이터 저장
	 * @param inputStream 수집한 데이터를 read할 inputStream
	 * @param fileName 저장 파일명
	 * @param logEncoding 저장로그 인코딩
	 * @param append append 여부
	 * @return 수집로그 크기
	 * @throws CollectException
	 */
	public long save(InputStream inputStream, String fileName, String logEncoding, boolean append) throws CollectException {
		return this.writeToFile(inputStream, fileName, logEncoding, append);
	}

	/**
	 * 수집데이터 저장
	 * @param sftpHandle 수집할 sftp handle ( input stream )
	 * @param fileName 저장 파일명
	 * @param logEncoding 저장로그 인코딩
	 * @param append append 여부
	 * @return 수집로그 크기
	 * @throws CollectException
	 */
	public long save(SFTPv3FileHandle sftpHandle, String fileName, String logEncoding, boolean append) throws CollectException {
		return this.writeToFile(sftpHandle, fileName, logEncoding, append);
	}

	/**
	 * 수집데이터 저장
	 * @param file 수집한 파일
	 * @param fileName 저장 파일명
	 * @param logEncoding 저장로그 인코딩
	 * @param append append 여부
	 * @return 수집로그 크기
	 * @throws CollectException
	 */
	public long save(File file, String fileName, String logEncoding, boolean append) throws CollectException {
		return this.writeToFile(file, fileName, logEncoding, append);
	}

	/**
	 * 모든 Protocol이 공통적으로 사용하는 Data 저장 메소드
	 * 파일명 : logpolicyId,dataSourceId,yyyyMMddHHmmss,utf-8,fileName,UUID 형태로 저장
	 * 수집로그 인코딩에 따라 해당 파일의 encoding을 UTF-8로 변환하여 저장한다.
	 * 저장이 완료되면 파일명 뒤에 .log를 붙여 FileSendThread 의 타겟이 되도록 한다.
	 * @param object 각 Protocol 별 저장할 데이터가 담겨 있는 Object
	 * 		  -> InputStream      : FtpDirScannerJob, SshShellExecuterJob
	 * 		  -> byte[]           : LocalFileTailerJob, LocalShellExecuterJob, SnmpJob
	 * 		  -> SFTPv3FileHandle : SFtpDirScannerJob
	 * 		  -> file			  : JdbcSqlExecuterJob, WizlookQueryJob, LocalFileScannerJob, ModbusPacketParserJob, AgentReceiver, TcpReceiver
	 * @param fileName 저장 파일명
	 * @param logEncoding 원본로그의 인코딩. UTF-8이 아닌경우 UTF-8로 변환
	 * @param append 파일 이어쓰기 여부
	 * @return 저장 파일 크기
	 * @throws IOException
	 */
	private long writeToFile(Object object, String fileName, String logEncoding, boolean append) throws CollectException {

		long startTime = System.currentTimeMillis();
		File savedFile = null;

		File collectDirectory = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR));

		try {
			if(!collectDirectory.exists()) collectDirectory.mkdirs();

			Date nowDate = new Date();
			String saveTime = logDf.format(nowDate);
			StringBuilder formattedFileName = new StringBuilder().append(logpolicyId)
																 .append(",").append(dataSourceId)
																 .append(",").append(saveTime)
																 .append(",").append(Strings.nullToEmpty(logEncoding))
																 .append(",").append(fileName)
																 .append(",").append(java.util.UUID.randomUUID().toString());
			if(object instanceof File) {
				savedFile = (File)object;
			} else {
				savedFile = saveObject(object, collectDirectory, formattedFileName.toString());
			}

			if(savedFile != null && savedFile.exists()) {
				long saveFileLen = savedFile.length();
				// 파일명.log 로 변경.. (FileSendThread의 타겟이 됨)

				savedFile.renameTo(new File(collectDirectory, formattedFileName.toString() + ".log"));
				logger.debug("Save collect file. Name=[" + formattedFileName.toString()+".log], size=[" + saveFileLen + "], elepsedTime=["+(System.currentTimeMillis()-startTime) + "]");
				loggingCollectHistoryForDashboard(nowDate.getTime(), fileName, saveFileLen);
				return saveFileLen;
			}
		} catch (IOException e) {
			throw new CollectException(CollectorCode.Code.FAIL_SAVE_LOG, e);
		}
		return -1;
	}

	/**
	 * Object의 instance형태에 따라 데이터를 파일로 저장
	 * @param object byte[], InputStream, SFTPv3FileHandle Object
	 * @param saveDirectory 파일 저장 디렉토리
	 * @param fileName 파일명
	 * @return 저장된 파일 반환
	 * @throws IOException
	 */
	private File saveObject(Object object, File saveDirectory, String fileName) throws IOException {
		BufferedOutputStream bos = null;
		File file = null;

		try {
			file = new File(saveDirectory, fileName);

			byte[] buffer = new byte[bufferSize];
			bos = new BufferedOutputStream(new FileOutputStream(file, false), bufferSize); // true => 이어쓰기, false => 파일새로생성

			// 파일 이어쓰기일 경우.. 파일크기가 0보다 크면 줄바꿈
			if(file.length() > 0) {
				bos.write(System.lineSeparator().getBytes());
			}

			// byte[] 데이터 저장
			if(object instanceof byte[]) {

				bos.write((byte[])object);

			// InputStream 데이터 저장
			} else if(object instanceof InputStream) {

				int readLength = 0;
	    	    while( (readLength = ((InputStream)object).read(buffer)) != -1 ) {
	    	    	bos.write(buffer, 0, readLength);
	    	    }

	    	// Sftp 데이터 저장
			} else if(object instanceof SFTPv3FileHandle) {

				SFTPv3FileHandle sftpFileHandle = (SFTPv3FileHandle)object;
				SFTPv3Client sftp = sftpFileHandle.getClient();

				long offset = 0;
				int readLength = 0;
				while ( (readLength = sftp.read(sftpFileHandle, offset, buffer, 0, buffer.length)) != -1 ) {
					bos.write(buffer, 0, readLength);
					offset += readLength;
				}
			} else {
				throw new IllegalArgumentException("invalid Object type.. ["+object.getClass()+"]");
			}
			bos.flush();
		} catch (IOException e) {
			throw e;
		} finally {
			if(bos != null) { try { bos.close(); } catch (IOException ie) { } }
		}
		return file;
	}

	/**
	 * log4j2의 collectHiostoryDashboardLogger를 통해 특정 파일에 로그를 남긴다.
	 * CollectHistoryDashboardInsertThread 가 해당 로그를 읽어 Dashboard용 DB에 적재한다.
	 * @param collectTime 수집시간
	 * @param fileName 파일명
	 * @param collectFileSize 파일사이즈
	 */
	public void loggingCollectHistoryForDashboard(long collectTime, String fileName, long collectFileSize) {
		if(!ConfigLoader.getInstance().getBoolean(Config.LOG_DB_ENABLED)) return;
		String collectHistoryLog = new StringBuilder().append(collectTime)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(logpolicyId)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(dataSourceId)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(fileName)
						   					   		  .append(AbstractCollect.COLLECTOR_SEPARATOR).append(collectFileSize).toString();
		LogManager.getLogger("collectHistoryDashboardLogger").info(collectHistoryLog);
	}
}