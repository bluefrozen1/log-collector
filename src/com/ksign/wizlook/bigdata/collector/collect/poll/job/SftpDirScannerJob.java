package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileHandle;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * 수집유형 SFTP 구현 클래스
 *  - SFTP Protocol을 통해 원격지 디렉토리에 존재하는 파일을 수집한다 ( 하위 디렉토리까지 체크 )
 * 이슈
 *  - osX의 경우 SFTP 연결도중 Exception이 발생하는 문제가 있음
 * @author byw
 */
public class SftpDirScannerJob extends PollJob {

	/** connection timeout millisecond */
	private int CONNECT_TIMEOUT = 60000;
	/** key exchange timeout millisecond */
	private int KEX_TIMEOUT = 0;
	/** SFTP Connection */
	private Connection conn = null;
	/** SFTP Client */
	private SFTPv3Client sftp = null;
	/** 디렉토리를 scan하여 가져올 파일 경로 목록 */
	private List<Map<String, String>> saveFilePathList = null;
	/** 원격지 File separator */
	private String remoteSystemFileSeparator = "/";

	@Override
	protected String validateParameter(Map<String, String> jobDataMap) {
		// 접속 host
		if (Strings.isNullOrEmpty(jobDataMap.get("dataServerHost"))) return "dataServerHost";
		// 접속 port
		if (Strings.isNullOrEmpty(jobDataMap.get("port"))) return "port";
		try {
			Integer.parseInt(jobDataMap.get("port"));
		} catch (NumberFormatException e) {
			return "port";
		}
		// 접속 user
		if (Strings.isNullOrEmpty(jobDataMap.get("userName"))) return "userName";
		// 스캔 대상 root 디렉토리
		if (Strings.isNullOrEmpty(jobDataMap.get("remoteDir"))) return "remoteDir";

		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobDataMap) throws Exception {

		// 접속 host
		String hostname = jobDataMap.get("dataServerHost");
		// 접속 port
		int port = Integer.parseInt(jobDataMap.get("port"));
		// 접속 user
		String username = jobDataMap.get("userName");
		// 접속 password
		String password = jobDataMap.get("password");
		// 스캔 대상 root 디렉토리
		String remoteRootDir = jobDataMap.get("remoteDir");
		// 스캔 대상 파일명 패턴
		Pattern fileNamePattern = getPattern(jobDataMap.get("fileNamePattern"));
		// 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
		int compareType = 2;
		if (!Strings.isNullOrEmpty(jobDataMap.get("metaInfoType"))) {
			compareType = Integer.parseInt(jobDataMap.get("metaInfoType"));
		}
		// 저장 완료 파일 삭제 여부
		String deleteYn = jobDataMap.get("deleteYn");

		try {

			// sftpClient 생성, 접속 및 로그인
			if(!initSftpClient(hostname, port, username, password)) {
				return false;
			}
			if(isInterruptedJob()) return false;

			// MetaFile 정보 조회
			File metaFile = getMetaFile();
			Map<String, String> metaInfo = getMetaInfo(metaFile);

			// 파일정보 목록
			saveFilePathList = new ArrayList<Map<String, String>>();

			collectLogger.loggingCollectDetailLog("[Find file] Target directory=[" + remoteRootDir + "]");
			// 저장할 파일 대상 목록 조회
			getSaveFileList(remoteRootDir, fileNamePattern, metaInfo, compareType);

			// 파일 목록 정렬 및 마지막 파일 제거
			sortSaveFilePathList(compareType);
			collectLogger.loggingCollectDetailLog("[Find file] Find file count=[" + saveFilePathList.size() + "]");

			// 파일 저장
			saveFile(metaFile, deleteYn);

			return true;
		} catch (Exception e) {
			throw e;
		} finally {
			if (sftp != null && sftp.isConnected()) {
				sftp.close();
			}
			if (conn != null) {
				conn.close();
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

		if (!metaFile.exists()) {
			if (metaFile.createNewFile()) {
				return metaFile;
			} else {
				throw new IOException("cant not create meta file" + metaFile.getAbsolutePath());
			}
		}
		return metaFile;
	}

	/**
	 * init SFTP
	 * @param hostname host address
	 * @param port port
	 * @param username login id
	 * @param password login pw
	 * @return init 성공 여부
	 * @throws IOException
	 */
	private boolean initSftpClient(String hostname, int port, String username, String password) throws IOException {

		collectLogger.loggingCollectDetailLog("[SFTP connect] host=[" + hostname + "], port=[" + port + "]");
		conn = new Connection(hostname, port);
		conn.connect(null, CONNECT_TIMEOUT, KEX_TIMEOUT);

		// 로그인
		if (!conn.authenticateWithPassword(username, password)) {
			collectLogger.loggingCollectDetailLog("[SFTP connect] Authentication failed");
			return false;
		}

		collectLogger.loggingCollectDetailLog("[SFTP connect] SUCCESS");
		sftp = new SFTPv3Client(conn);
		return true;
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

	/**
	 * 하위 디렉토리를 모두 스캔하여 존재하는 모든 파일을 리스트업
	 * @param remoteDir 현재 디렉토리
	 * @param fileNamePattern 스캔 대상 파일명 패턴
	 * @param metaInfoMap 파일 절대경로, 파일 수정시간 등 메타정보
	 * @param compareType 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
	 * @throws IOException
	 * @throws Exception
	 */
	private void getSaveFileList(String remoteDir, Pattern fileNamePattern, Map<String, String> metaInfoMap, int compareType) throws IOException {

		List<SFTPv3DirectoryEntry> remoteFileList = sftp.ls(remoteDir);
		if(remoteFileList == null) return;

		FileComparator comparator = new FileComparator(FileComparator.SORT_TYPE_ASC, compareType);
		for (SFTPv3DirectoryEntry remoteFileEntry : remoteFileList) {

			if (remoteFileEntry.attributes.isDirectory()
					&& !(".".equals(remoteFileEntry.filename) || "..".equals(remoteFileEntry.filename))) {

				// 디렉토리일 경우 재귀호출을 통해 하위 파일들을 다시 검색
				// TODO : / 대신 원격지 시스템의 file separator 를 입력해야 함
				getSaveFileList(remoteDir + remoteSystemFileSeparator + remoteFileEntry.filename, fileNamePattern, metaInfoMap, compareType);

			} else if (remoteFileEntry.attributes.isRegularFile()) {
				// 1. 파일 패턴 비교 ( fileNamePattern이 null이 아닐 경우만 )
				// 2. 메타정보 비교
				// 3. 저장 대상 목록에 추가
				if (fileNamePattern == null || fileNamePattern.matcher(remoteFileEntry.filename).matches()) {

					String currentFileAbsolutePath = remoteDir + remoteSystemFileSeparator + remoteFileEntry.filename;

					HashMap<String, String> fileInfoMap = new HashMap<String, String>();
					fileInfoMap.put("absolutePath", currentFileAbsolutePath);
					fileInfoMap.put("fileName", remoteFileEntry.filename);
					fileInfoMap.put("lastModifiedDate", remoteFileEntry.attributes.mtime.toString());

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

	/**
	 * 저장 대상 목록에 추가된 파일들을 로컬디렉토리에 저장한다.
	 * @param remoteRootDir 원격 디렉토리
	 * @param saveDir 저장할 로컬 디렉토리
	 * @throws IOException 
	 */
	private void saveFile(File metaFile, String deleteYn) throws Exception {
		collectLogger.loggingCollectDetailLog("[GET file] START");
		for (Map<String, String> fileInfoMap : saveFilePathList) {
			if(isInterruptedJob()) break;
			if (!sftp.isConnected()) {
				throw new IOException("sftp Connection disconnected");
			}

			SFTPv3FileHandle sftpFileHandle = null;
			try {
				// open remote file with read only
				if(fileInfoMap == null) continue;
				sftpFileHandle = sftp.openFileRO(fileInfoMap.get("absolutePath"));                                                                                                                                                                                                                                           

				long saveFileSize = super.save(sftpFileHandle, fileInfoMap.get("fileName"), collectLogEncoding, false);

				if (saveFileSize > -1) {
					collectLogger.loggingCollectDetailLog("[GET file] SUCCESS. file=[" + fileInfoMap.get("absolutePath") + "], size=[" + saveFileSize + "]");

					if ("Y".equals(deleteYn)) {
						try {
							sftp.rm(fileInfoMap.get("absolutePath"));
						} catch (IOException e) {
							logger.error(this.getClass().getSimpleName(), e);
							collectLogger.loggingCollectDetailLog("[GET file]", e);
						}
						collectLogger.loggingCollectDetailLog("[GET file] Delete remote file. file=[" + fileInfoMap.get("absolutePath") + "]");
					}
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
						writer.close();
					}
				}

			} finally {
				if (sftpFileHandle != null && !sftpFileHandle.isClosed()) {
					try {
						sftp.closeFile(sftpFileHandle);
					} catch (IOException e) {
						logger.error(this.getClass().getSimpleName(), e);
					}
				}
			}
		}
		collectLogger.loggingCollectDetailLog("[GET file] END");
	}

	/**
	 * compareType에 따라 파일을 정렬한다
	 * @param compareType 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
	 */
	private void sortSaveFilePathList(int compareType) {
		if(saveFilePathList.size() > 0) {
			Collections.sort(saveFilePathList, new FileComparator(FileComparator.SORT_TYPE_ASC, compareType));

			if (!ConfigLoader.getInstance().getBoolean(Config.COLLECT_LAST_MODIFIED_FILE)) {
				// 수집대상 파일중 마지막 file을 제외하고 수집한다.
				// 마지막 file이 아직 write중일 수도 있음을 고려
				saveFilePathList.remove(saveFilePathList.size() - 1);
			}
		}
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
