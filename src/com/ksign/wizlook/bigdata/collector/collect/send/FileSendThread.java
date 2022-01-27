package com.ksign.wizlook.bigdata.collector.collect.send;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.itf.avro.CollectorInterfaceService;
import com.ksign.wizlook.bigdata.collector.itf.avro.InterfaceException;
import com.ksign.wizlook.common.util.FileUtil;
/**
 * 수집된 File을 Engine으로 전달하는 클래스
 * @author byw
 */
public class FileSendThread implements Runnable {
	/** send thread count */
	private final int sendThreadCount = 1;
	/** send queue size */
	private final int sendQueueSize = 10;
	/** scan target directory */
	private final File watchDir;
	/** send queue */
	private final BlockingQueue<File> sendQueue;
	/** send thread pool */
	private final ExecutorService executorService;
	/** logger */
	private final Logger logger = LogManager.getLogger();
	private final String engineEncoding = "UTF-8";

	private final String COLLECT_FILE_ENCODING_TYPE_JAVA = "1";
	private final String COLLECT_FILE_ENCODING_TYPE_SHELL = "2";

	public FileSendThread() throws IOException {
		this.watchDir = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR));
		if(watchDir != null && !watchDir.exists()) {
			if(!watchDir.mkdirs()) throw new IOException("It failed to create watch directory... path=[" + watchDir.getAbsolutePath() + "]");
		}
		this.sendQueue = new ArrayBlockingQueue<File>(sendQueueSize);

		executorService = Executors.newFixedThreadPool(sendThreadCount);
		for(int i=0; i<sendThreadCount; i++) { 
			executorService.execute(new FileSender());
		}

		// 서버 비정상 종료 시 처리되지 못했던 전송대기 파일들 전송처리
		new Thread(new Runnable() {
			@Override
			public void run() {
				File sendLogDir = new File(ConfigLoader.getInstance().get(Config.SEND_LOG_DIR));
				if(sendLogDir != null && !sendLogDir.exists()) sendLogDir.mkdirs();
				File[] sendFileList = sendLogDir.listFiles(new LogFileNameFilter());
				for(File sendFile : sendFileList) {
					try { sendQueue.put(sendFile); } catch (InterruptedException e) { logger.error(this.getClass().getSimpleName(), e); }
				}
			}
		}).start();
	}

	@Override
	public void run() {
		// 1. 수집된 파일 저장 디렉토리 스캔
		// 2. 스캔된 파일을 전송디렉토리로 이동 후 큐에 삽입
		logger.info("	Start " + this.getClass().getSimpleName());
		while(!Thread.currentThread().isInterrupted()) {
			File[] fileList = watchDir.listFiles(new LogFileNameFilter());
			if(fileList == null || fileList.length == 0) {
				try {
					Thread.sleep(1000); 
				} catch (InterruptedException e) { 
					Thread.currentThread().interrupt();
				}
				continue;
			}

			for(File collectedFile : fileList) {
				try {
					File sendFile = new File(ConfigLoader.getInstance().get(Config.SEND_LOG_DIR) + File.separator + collectedFile.getName());
					if(collectedFile.renameTo(sendFile)) {
						sendQueue.put(sendFile);
					}
					// TODO : rename 실패처리
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
		logger.info("	Stop " + this.getClass().getSimpleName());
		executorService.shutdownNow();
	}

	class LogFileNameFilter implements FilenameFilter {
		@Override
		public boolean accept(File file, String fileName) {
			if(fileName.endsWith(".log")) return true;
			else return false;
		}
	}

	class FileSender implements Runnable {

		@Override
		public void run() {
			// 1. 큐에서 전송 대상 파일 추출
			// 2. 저장된 파일명에서 정보 추출 ( 파일명 형식 : logpolicyId,dataSourceId,201505280012,EUC-KR,fileName )
			// 3. 마스터노드에 조회 ( 어떤 노드로 로그파일을 전송할지 )
			// 4. 로그파일 전송
			// 5. 백업 디렉토리로 이동 ( properties의 backup.log가 false면 삭제 )
			while(!Thread.currentThread().isInterrupted()) {
 				File sendFile = null;
				try {
					sendFile = sendQueue.take();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}

				String originFilePath = sendFile.getAbsolutePath();
				try {
					String[] formattedNameArr = sendFile.getName().split(",", 5);
					String logpolicyId = formattedNameArr[0];
					String dataSourceId = formattedNameArr[1];
					String saveDate = formattedNameArr[2];
					String logEncoding = formattedNameArr[3];
					String fileName = formattedNameArr[4];

					boolean sendResult = true;
					boolean isUtf8 = true;
					if(ConfigLoader.getInstance().getBoolean(Config.SEND_LOG_ENABLED)) {
						// 수집된 로그의 인코딩이 UTF-8이 아닌 경우 UTF-8로 변환
						// 수집 단계에서 이미 UTF-8로 수집되었거나 변환해서 수집하였다면 이 단계에서 굳이 인코딩을 바꾸지 않는다.
						// 파일명의 4번째 값이 현재 파일의 인코딩이다.
						boolean convertEncoding = ConfigLoader.getInstance().getBoolean(Config.COLLECT_FILE_ENCODING_CONVERT);
 						if(convertEncoding && !Strings.isNullOrEmpty(Strings.nullToEmpty(logEncoding)) && !engineEncoding.equals(logEncoding)) {
							String engineEncodingFilePath = new StringBuilder().append(sendFile.getParentFile().getAbsolutePath()).append(File.separator)
																	 .append(logpolicyId).append(",")
																	 .append(dataSourceId).append(",")
																	 .append(saveDate).append(",")
																	 .append(engineEncoding).append(",")
																	 .append(fileName).toString();

							// java로 인코딩 변환
							if(COLLECT_FILE_ENCODING_TYPE_JAVA.equals(ConfigLoader.getInstance().get(Config.COLLECT_FILE_ENCODING_CONVERT_TYPE))) {
								convertFileEncoding(sendFile, logEncoding, new File(engineEncodingFilePath), engineEncoding);

							// shell 을 통해서 인코딩 변환
							} else if(COLLECT_FILE_ENCODING_TYPE_SHELL.equals(ConfigLoader.getInstance().get(Config.COLLECT_FILE_ENCODING_CONVERT_TYPE))) {
								String convertEncodingShellPath = ConfigLoader.getInstance().get(Config.COLLECT_FILE_ENCODING_CONVERT_SHELL_PATH);
								Process process = Runtime.getRuntime().exec(convertEncodingShellPath + " " 
										+ logEncoding + " " 
										+ engineEncoding + " "  
										+ sendFile.getAbsolutePath() + " "
										+ engineEncodingFilePath);
								process.waitFor();

							// java로 인코딩 변환
							} else {
								convertFileEncoding(sendFile, logEncoding, new File(engineEncodingFilePath), engineEncoding);
							}

							File engineEncodingFile = new File(engineEncodingFilePath);
							if(engineEncodingFile != null && engineEncodingFile.exists()) {
								sendFile.delete();
								sendFile = engineEncodingFile;
								originFilePath = sendFile.getAbsolutePath();
							} else {
								String convertType = COLLECT_FILE_ENCODING_TYPE_SHELL.equals(ConfigLoader.getInstance().get(Config.COLLECT_FILE_ENCODING_CONVERT_TYPE)) ? "SHELL" : "JAVA";
								logger.error("Convert encoding Error.. convertType=[" + convertType + "], targetFile=[" + sendFile + "] fromEncoding=[" + logEncoding + "], toEncoding=[" + engineEncoding + "]");
								isUtf8 = false;
							}
						}
						if(isUtf8) {
							// 수집된 로그를 노드로 전송
							sendResult = sendFileToNode(sendFile, logpolicyId);
						} else {
							sendResult = false;
						}
					}

					// backup.log=true 일 경우
					if(ConfigLoader.getInstance().getBoolean(Config.BACKUP_LOG_ENABLED)) {
						if(sendResult) {
							// backup directory로 이동
							backupFile(sendFile, ConfigLoader.getInstance().get(Config.BACKUP_LOG_BASE_DIR), logpolicyId, dataSourceId, saveDate, fileName);
						} else {
							// error directory로 이동
							backupFile(sendFile, ConfigLoader.getInstance().get(Config.COLLECT_ERROR_BASE_DIR), logpolicyId, dataSourceId, saveDate, fileName);
						}
					}
				} catch(Exception e) {
					logger.error(this.getClass().getSimpleName(), e);
				} finally {
					File originFile = new File(originFilePath);
					if(originFile != null && originFile.exists()) originFile.delete();
				}
			}
		}

		/**
		 * properties의 backup.log 플래그에 따라 backup directory로 이동하거나 삭제
		 * 저장 시 경로 : backup.log.base.dir / logpolicyMappingKey / dataSourceId / 20150528 / fileName
		 * @param sendFile node로 전송된 로그
		 * @param logpolicyId 정책아이디
		 * @param dataSourceId 데이터소스아이디
		 * @param saveDate 저장시간
		 * @param fileName 파일명
		 */
		private void backupFile(File sendFile, String baseDir, String logpolicyId, String dataSourceId, String saveDate, String fileName) {
			File backupFile = new File(new StringBuilder().append(baseDir)
												  	   	  .append(File.separator).append(logpolicyId)
												  	   	  .append(File.separator).append(dataSourceId)
												  	   	  .append(File.separator).append(saveDate.substring(0, 8))
												  	   	  .append(File.separator).append(fileName).toString());

			if(!backupFile.getParentFile().exists()) {
				if(!backupFile.getParentFile().mkdirs()) {
					logger.error(this.getClass().getSimpleName() + ". It failed to create directory.. path=[" + backupFile.getParent() + "]");
				}
			}
//				if(ConfigLoader.getInstance().getBoolean(Config.BACKUP_LOG_COMPRESSION)) {
//				}
			sendFile.renameTo(backupFile);
		}

		/**
		 * 노드 디렉토리로 파일 복사
		 * TODO : 마스터노드에 전송 대상 노드 조회 요청 및 해당 노드로 전송 로직으로 변경 
		 * @param sendFile 전송 대상 파일
		 * @param logpolicyId 정책아이디
		 * @return 저장 결과
		 * @throws InterfaceException 수집로그 저장 node 정보 조회 실패
		 * @throws CollectException 수집로그 저장 실패
		 */
		private boolean sendFileToNode(File sendFile, String logpolicyId) throws InterfaceException, CollectException {

			String fileName = sendFile.getName();
			if(fileName.endsWith(".log")) fileName = fileName.substring(0, fileName.length() - ".log".length());

			List<Map<String, String>> policyNodeMapList = CollectorInterfaceService.INSTANCE.getPolicyNode(logpolicyId);

			if(policyNodeMapList != null) {
				for(Map<String, String> policyNodeMap : policyNodeMapList) {
					String logSaveDirectory = policyNodeMap.get("logBaseDirectory") + File.separator + policyNodeMap.get("logpolicyMappingKey");
					File saveFile = new File(logSaveDirectory + File.separator + fileName);
					try {
						FileUtil.transferTo(sendFile, saveFile);
						File indexTargetFile = new File(logSaveDirectory + File.separator + saveFile.getName() + ".log");
						boolean result = saveFile.renameTo(indexTargetFile);
						logger.debug(new StringBuilder().append("Send file to node.. Logpolicy id=[").append(logpolicyId)
													    .append("], Save result=[").append(result)
													    .append("], Node id=[").append(policyNodeMap.get("nodeId"))
													    .append("], File=[").append(indexTargetFile.getAbsolutePath())
													    .append("], Size=[").append(indexTargetFile.length()).append("]").toString());
					} catch (IOException e) {
						logger.error(this.getClass().getSimpleName(), e);
						throw new CollectException(CollectorCode.Code.FAIL_SEND_LOG_TO_NODE);
					}
				}
			}
			return true;
		}
	}

	/**
	 * 인코딩 변경
	 * @param fromFile 인코딩 변경 전 파일
	 * @param fromEncoding 변경 전 인코딩
	 * @param toFile 인코딩 변경 후 파일
	 * @param toEncoding 변경 인코딩
	 */
	private void convertFileEncoding(File fromFile, String fromEncoding, File toFile, String toEncoding){
		BufferedReader reader = null;
		BufferedWriter writer = null;
		try {
			long l = System.currentTimeMillis();
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(fromFile), fromEncoding));

			//기존파일에 엎어쓴다.
			writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(toFile), toEncoding));

			String readLine = null;
			while ((readLine = reader.readLine()) != null) {
				writer.write(readLine);
				writer.newLine();
			}
			writer.flush();
logger.debug("convertFileEncoding time=[" + (System.currentTimeMillis() - l)+ "]");

		} catch (Exception e) {
        	logger.error(this.getClass().getSimpleName(), e);
        } finally {
        	if(reader != null) try { reader.close(); } catch (IOException e) {}
        	if(writer != null) try { writer.close(); } catch (IOException e) {}
        }
	}
}