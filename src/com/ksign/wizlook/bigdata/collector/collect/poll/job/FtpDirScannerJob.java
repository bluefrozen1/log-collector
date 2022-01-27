package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPListParseEngine;
import org.apache.commons.net.ftp.FTPReply;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.FtpConnectMode;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * 수집유형 FTP 구현 클래스
 *  - FTP Protocol을 통해 원격지 디렉토리에 존재하는 파일을 수집한다 ( 하위 디렉토리까지 체크 )
 * @author byw
 */
public class FtpDirScannerJob extends PollJob {
	/** FTP Client */
	FTPClient ftpClient = null;
	/** 디렉토리 scan시 한번에 가져올 파일 count */
	private int DIR_PAGE_COUNT 	= 300;
	/** 디렉토리를 scan하여 가져올 파일 경로 목록 */
	private List<Map<String, String>> saveFilePathList = null;
	/** Connection timeout millisecond */
	private int CONNECTION_TIMEOUT = 5000;
	/** 원격지 File separator */
	private String remoteSystemFileSeparator = "/";

	@Override
	protected String validateParameter(Map<String, String> jobDataMap) {

		// 접속 host
		if(Strings.isNullOrEmpty(jobDataMap.get("dataServerHost"))) return "dataServerHost";
		// 접속 port
		if(Strings.isNullOrEmpty(jobDataMap.get("port"))) return "port";
		try {
			Integer.parseInt(jobDataMap.get("port"));
		} catch (NumberFormatException e) {
			return "port";
		}
		// Active or Passive mode 여부
		if(Strings.isNullOrEmpty(jobDataMap.get("connectMode"))) return "connectMode";
		// 접속 user
		if(Strings.isNullOrEmpty(jobDataMap.get("userName"))) return "userName";
		// 스캔 대상 root 디렉토리
		if(Strings.isNullOrEmpty(jobDataMap.get("remoteDir"))) return "remoteDir";

		return null;
	}

	@Override
	public boolean collect(Map<String, String> jobDataMap) throws Exception {

		// 접속 host
		String hostname 	= jobDataMap.get("dataServerHost");
		// 접속 port
		int port 			= Integer.parseInt(jobDataMap.get("port"));
		// Active or Passive mode 여부
		String connectMode = jobDataMap.get("connectMode");
		// 접속 user
		String username 	= jobDataMap.get("userName");
		// 접속 password
		String password 	= jobDataMap.get("password");
		// 스캔 대상 root 디렉토리
		String remoteRootDir 	= jobDataMap.get("remoteDir");
		// 스캔 대상 파일명 패턴
		Pattern fileNamePattern = getPattern(jobDataMap.get("fileNamePattern"));
		// 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
		int compareType = 2;
		if(!Strings.isNullOrEmpty(jobDataMap.get("metaInfoType"))) {
			compareType = Integer.parseInt(jobDataMap.get("metaInfoType"));
		}
		// 스캔 완료 파일 삭제 여부
		String deleteYn = jobDataMap.get("deleteYn");

		try {
			// ftpClient 생성, 접속 및 로그인
			if(!initFtpClient(hostname, port, username, password, connectMode)) {
				return false;
			}
			if(isInterruptedJob()) return false;

		    // MetaFile 정보 조회
		    File metaFile = getMetaFile();
		    Map<String, String> metaInfoMap = getMetaInfo(metaFile);
		    
		    // 파일정보 목록
		    saveFilePathList = new ArrayList<Map<String, String>>();

		    // 저장할 파일 대상 목록 조회
	    	getSaveFileList(remoteRootDir, fileNamePattern, metaInfoMap, compareType);

	    	// 파일 목록 정렬
	    	sortSaveFilePathList(compareType);
	    	collectLogger.loggingCollectDetailLog("[Find file] Find file count=[" + saveFilePathList.size() + "]");

	    	// 파일 저장
	    	saveFile(metaFile, deleteYn);

	    	return true;
		} catch (Exception e) {
			throw e;
		} finally {
			if(ftpClient != null && ftpClient.isConnected()) {
				try { ftpClient.logout(); } catch(IOException e) { logger.error(this.getClass().getSimpleName(), e); }
				try { ftpClient.disconnect(); } catch(IOException e) { logger.error(this.getClass().getSimpleName(), e); }
			}
		}
	}

	/**
	 * ftpClient 생성, 접속 및 로그인
	 * @param hostname 연결 호스트 명
	 * @param port 연결 포트 명
	 * @param username 로그인 사용자 계정
	 * @param password 로그인 사용자 비밀번호
	 * @throws SocketException
	 * @throws IOException
	 */
	private boolean initFtpClient(String hostname, int port, String username, String password, String connectMode) throws SocketException, IOException {

		ftpClient = new FTPClient();

		ftpClient.setConnectTimeout(CONNECTION_TIMEOUT); // connection time out
		collectLogger.loggingCollectDetailLog("[FTP connect] host=[" + hostname + "], port=[" + port + "]");
		ftpClient.connect(hostname, port);

		ftpClient.setSoTimeout(CONNECTION_TIMEOUT);		// socket time out
		ftpClient.setDataTimeout(CONNECTION_TIMEOUT);	// data connection time out
		ftpClient.setSoLinger(true, 0); 				// socket 즉시 종료하기 위한 option

		if(FtpConnectMode.PASSIVE.toString().equals(connectMode)) {
			collectLogger.loggingCollectDetailLog("[FTP connect] connectMode =[" + connectMode + "]");
			ftpClient.enterLocalPassiveMode(); // default active mode
		}

		// 접속 유효성 체크
		if(!FTPReply.isPositiveCompletion(ftpClient.getReplyCode())) {
			collectLogger.loggingCollectDetailLog("[FTP connect] FAIL. ReplyCode=[" + ftpClient.getReplyCode() + "]");
			return false;
		}
		collectLogger.loggingCollectDetailLog("[FTP connect] SUCCESS");

		// 로그인
		collectLogger.loggingCollectDetailLog("[FTP login] username=[" + username + "]");
		if(!ftpClient.login(username, password)) {
			collectLogger.loggingCollectDetailLog("[FTP login] FAIL");
			return false;
		}
		collectLogger.loggingCollectDetailLog("[FTP login] SUCCESS");

//		FTPListParseEngine ftpListParseEngine = ftpClient.initiateListParsing(remoteDir); // 목록을 나타낼 디렉토리
//		if(!ftpListParseEngine.hasNext()) {
//			System.out.println("Directory is not exist or empty");
//			return;
//		}

		try {
			// 파일다운 관련 정보 설정
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpClient.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);
		} catch (IOException e) {
			logger.error(this.getClass().getSimpleName(), e);
		}
		return true;
	}

	/**
	 * 저장 대상 목록에 추가된 파일들을 로컬디렉토리에 저장한다.
	 * @param remoteRootDir 원격 디렉토리
	 * @param saveDir 저장할 로컬 디렉토리
	 * @throws IOException 
	 */
	private void saveFile(File metaFile, String deleteYn) throws Exception {
		collectLogger.loggingCollectDetailLog("[GET file] START.");
		for (Map<String, String> fileInfoMap : saveFilePathList) {
			if(isInterruptedJob()) break;
			if(fileInfoMap == null) continue;
			if(!ftpClient.isConnected()) {
				throw new IOException("ftp Connection disconnected");
			}

			InputStream is = ftpClient.retrieveFileStream(fileInfoMap.get("absolutePath"));

			long saveFileSize = super.save(is, fileInfoMap.get("fileName"), collectLogEncoding, false);

			if("Y".equals(deleteYn) && saveFileSize > -1) {
				try {
					ftpClient.deleteFile(fileInfoMap.get("absolutePath"));
					collectLogger.loggingCollectDetailLog("[GET file] Delete remote file. file=[" + fileInfoMap.get("absolutePath") + "]");
				} catch (IOException e) {
					collectLogger.loggingCollectDetailLog("[GET file]", e);
				}
			}

			ftpClient.completePendingCommand();

			if (saveFileSize > -1) {
				collectLogger.loggingCollectDetailLog("[GET file] SUCCESS. file=[" + fileInfoMap.get("absolutePath") + "], size=[" + saveFileSize + "]");
			} else {
				collectLogger.loggingCollectDetailLog("[GET file] FAIL. file=[" + fileInfoMap.get("absolutePath") + "]");
				// TODO : 실패한 파일 처리
			}
			// 어떤 파일까지 진행하였는지 MetaFile 정보 갱신
			if (fileInfoMap != null && metaFile != null) {
				FileWriter writer = null;
				try {
					writer = new FileWriter(metaFile);
					writer.write(fileInfoMap.get("absolutePath") + "|" + fileInfoMap.get("lastModifiedDate"));
					writer.flush();
				} catch (Exception e) {
					logger.error(this.getClass().getSimpleName(), e);
				} finally {
					if(writer != null) writer.close();
				}
			}
    	}
		collectLogger.loggingCollectDetailLog("[GET file] END.");
	}
	
	/**
	 * compareType에 따라 파일을 정렬한다
	 * @param compareType 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
	 */
	private void sortSaveFilePathList(int compareType) {
		if(saveFilePathList.size() > 0) {
			Collections.sort(saveFilePathList, new FileComparator(FileComparator.SORT_TYPE_ASC, compareType));

			if (!ConfigLoader.getInstance().getBoolean("collect.last.modified.file")) {
				// 수집대상 파일중 마지막 file을 제외하고 수집한다.
				// 마지막 file이 아직 write중일 수도 있음을 고려
				saveFilePathList.remove(saveFilePathList.size() - 1);
			}
		}
	}
	
	/**
	 * 하위 디렉토리를 모두 스캔하여 존재하는 모든 파일을 리스트업
	 * @param remoteDir 현재 디렉토리
	 * @param fileNamePattern 스캔 대상 파일명 패턴
	 * @param metaInfoMap 파일 절대경로, 파일 수정시간 등 메타정보
	 * @param compareType 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
	 * @throws IOException
	 * @throws Exception
	 */
	private void getSaveFileList(String remoteDir, Pattern fileNamePattern, Map<String, String> metaInfoMap, int compareType) throws IOException, Exception {

		// 목록을 나타낼 디렉토리
		FTPListParseEngine ftpListParseEngine = ftpClient.initiateListParsing(remoteDir);

		while (ftpListParseEngine.hasNext()) {
			if(isInterruptedJob()) break;
			// 페이징 처리
			FTPFile[] ftpfiles = ftpListParseEngine.getNext(DIR_PAGE_COUNT);
		
	        if (ftpfiles != null) {
	        	FileComparator comparator = new FileComparator(FileComparator.SORT_TYPE_ASC, compareType);
	            for (int i = 0; i < ftpfiles.length; i++) {
	            	FTPFile ftpFile = ftpfiles[i];
	            	// 디렉토리일 경우 재귀호출을 통해 하위 파일들을 다시 검색
	            	// TODO : / 대신 원격지 시스템의 file separator 를 입력해야 함
	            	if(ftpFile.isDirectory()) {
	            		getSaveFileList(remoteDir + remoteSystemFileSeparator + ftpFile.getName(),
	            						fileNamePattern,
	            						metaInfoMap,
	            						compareType);

	            	// 파일일 경우
	            	} else if (ftpFile.isFile()){
	            		// 1. 파일 패턴 비교 ( fileNamePattern이 null이 아닐 경우만 )
	            		// 2. 메타정보 비교
	            		// 3. 저장 대상 목록에 추가
	            		
	            		if (fileNamePattern == null || fileNamePattern.matcher(ftpFile.getName()).matches()) {

	            			String currentFileAbsolutePath = remoteDir + remoteSystemFileSeparator + ftpFile.getName();
	            			String modifiedDate = ftpClient.getModificationTime(currentFileAbsolutePath).split(" ")[1].substring(0, 14);

	            			Map<String, String> fileInfoMap = new HashMap<String, String>();
    						fileInfoMap.put("absolutePath", currentFileAbsolutePath);
    						fileInfoMap.put("fileName", ftpFile.getName());
    						fileInfoMap.put("lastModifiedDate", modifiedDate);

	            			if (metaInfoMap != null) {
	            				int result = comparator.compare(fileInfoMap, metaInfoMap);
	        					if (result > 0) {
	        						saveFilePathList.add(fileInfoMap);
	        					}
	            			} else {
	    						saveFilePathList.add(fileInfoMap);
	            			}
	            		}
	            	}
	            }
	        }
		}
	}

	/**
	 * MetaFile을 반환. 없을경우 생성
	 * @return MetaFile
	 * @throws IOException 
	 */
	private File getMetaFile() throws IOException {

		File metaFileDirectory = new File(ConfigLoader.getInstance().get(Config.COLLECT_METAINFO_BASE_DIR) + File.separator + logpolicyId);
		if (!metaFileDirectory.exists()) metaFileDirectory.mkdirs();

		File metaFile = new File(metaFileDirectory  + File.separator + dataSourceId + "_meta.info");

		if(!metaFile.exists()) {
			if(metaFile.createNewFile()) {
				return metaFile; 
			} else {
				throw new IOException("cant not create meta file" + metaFile.getAbsolutePath());
			}
		}
		return metaFile;
	}

	/**
	 * MetaFile에서 메타정보를 읽어서 반환
	 * @param metaFile
	 * @return Meta정보반환
	 */
	private Map<String, String> getMetaInfo(File metaFile) {

		if(metaFile == null) {
			return null;
		}

		BufferedReader in = null;
		if(metaFile.exists()) {
			try {
				in = new BufferedReader(new FileReader(metaFile));
				String metaInfo = in.readLine();
				if(Strings.isNullOrEmpty(metaInfo)) return null;
				String[] arr = metaInfo.split("\\|");
				Map<String, String> metaInfoMap = new HashMap<String, String>();
				metaInfoMap.put("absolutePath", arr[0]);
				metaInfoMap.put("lastModifiedDate", arr[1]);
				return metaInfoMap;
			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
			} finally {
				if (in != null) try {in.close();} catch (Exception e) {}
			}
		}
		return null;
	}

	private Pattern getPattern(String patternStr) throws PatternSyntaxException {
		try {
			if (!Strings.isNullOrEmpty(patternStr)) {
				return Pattern.compile(patternStr);
			}
		} catch (PatternSyntaxException e) {
			throw e;
		}
		return null;
	}

	public class FileComparator implements Comparator<Map<String, String>> {

		public static final int SORT_TYPE_ASC = 1;
		public static final int SORT_TYPE_DESC = 2;

		public static final int COMPARE_TYPE_NAME = 1;
		public static final int COMPARE_TYPE_DATE = 2;

		private int sortType = 0;
		private int compareType = 0;

		public FileComparator(int sortType, int compareType) {
			this.sortType = sortType;
			this.compareType = compareType;
		}

		@Override
		public int compare(Map<String, String> fileInfoMap1, Map<String, String> fileInfoMap2) {
			int rslt = 0;
			String compareKey = "absolutePath";
			String subCompareKey = "lastModifiedDate";
			if(compareType == 1) {
				compareKey = "absolutePath";
				subCompareKey = "lastModifiedDate";
			} else if(compareType == 2) {
				compareKey = "lastModifiedDate";
				subCompareKey = "absolutePath";
			}

			switch (sortType) {
				case SORT_TYPE_ASC:
					rslt = fileInfoMap1.get(compareKey).compareTo(fileInfoMap2.get(compareKey));
					if(rslt == 0) {
						rslt = fileInfoMap1.get(subCompareKey).compareTo(fileInfoMap2.get(subCompareKey));
					}
					break;
				case SORT_TYPE_DESC:
					rslt = fileInfoMap2.get(compareKey).compareTo(fileInfoMap1.get(compareKey));
					if(rslt == 0) {
						rslt = fileInfoMap1.get(subCompareKey).compareTo(fileInfoMap2.get(subCompareKey));
					}
					break;
			}
			return rslt;
		}
	}
}