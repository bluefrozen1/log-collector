package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.base.Strings;
import com.ksign.processor.extractor.PacketExtractor;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * 수집유형 PACKET_PARSER_MODBUS 구현 클래스
 *  - 로컬에 존재하는 디렉토리를 스캔하여 .pcap파일을 찾아 파싱하여 저장한다. 
 * @author byw
 */
public class ModbusPacketParserJob extends PollJob {

	private List<HashMap<String, String>> saveFilePathList = null;

	@Override
	protected String validateParameter(Map<String, String> jobConfigDataMap) {
		if(Strings.isNullOrEmpty(jobConfigDataMap.get("scanDir"))) return "scanDir";
		if(Strings.isNullOrEmpty(jobConfigDataMap.get("deleteYn"))) return "deleteYn";
		if(Strings.isNullOrEmpty(jobConfigDataMap.get("metaInfoType"))) return "metaInfoType";
		if(!Strings.isNullOrEmpty(jobConfigDataMap.get("fileNamePattern"))
				&& !checkPattern(jobConfigDataMap.get("fileNamePattern"))) {
			return "fileNamePattern";
		}
		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobConfigDataMap) throws Exception {
		String scanDir = jobConfigDataMap.get("scanDir");
		String fileNamePattern = jobConfigDataMap.get("fileNamePattern");
		String deleteYn = jobConfigDataMap.get("deleteYn");
		// 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
		int compareType = 2;
		if(!Strings.isNullOrEmpty(jobConfigDataMap.get("metaInfoType"))) {
			compareType = Integer.parseInt(jobConfigDataMap.get("metaInfoType"));
		}

		try {
			File scanDirectory = new File(scanDir);
			if(scanDirectory == null || !scanDirectory.exists()) {
				collectLogger.loggingCollectDetailLog("[Scan directory] Is not Exist directory. directory=[" + scanDir + "]");
				return false;
			}

			// MetaFile 정보 조회
			File metaFile = getMetaFile();
			Map<String, String> metaInfo = getMetaInfo(metaFile);

			 // 파일정보 목록
		    saveFilePathList = new ArrayList<HashMap<String, String>>();

			// 저장할 파일 대상 목록 조회
		    collectLogger.loggingCollectDetailLog("[Scan Directory] Target directory=[" + scanDirectory.getAbsolutePath() + "]");
	    	getSaveFileList(scanDirectory, fileNamePattern, metaInfo, compareType);

	    	// 파일 목록 정렬 및 마지막 파일 제거
	    	sortFileList(compareType);
	    	collectLogger.loggingCollectDetailLog("[Scan Directory] target file count=[" + saveFilePathList.size() + "]");

	    	getFile(deleteYn, metaFile);

	    	return true;
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * saveFilePathList 에 등록된 패킷파일을 파싱하여 저장, 메타정보갱신, 삭제 처리
	 * @param deleteYn 삭제 여부
	 * @param metaFile 메타정보 기록 파일
	 * @throws IOException
	 * @throws CollectException
	 */
	private void getFile(String deleteYn, File metaFile) throws IOException, CollectException {
		collectLogger.loggingCollectDetailLog("[GET file] START");
		PacketExtractor extractor = new PacketExtractor();
		
		for(Map<String, String> fileInfoMap : saveFilePathList) {
			if(isInterruptedJob()) break;
			File targetFile = new File(fileInfoMap.get("absolutePath"));
			File saveFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator + "PACKET_PARSE_MODBUS," + logpolicyId + "," + dataSourceId + "," + UUID.randomUUID().toString());
			String targetFileName = targetFile.getName();

			try {
				collectLogger.loggingCollectDetailLog("[Modbus Parsing] Start. file=[" + fileInfoMap.get("absolutePath") + "]");
				extractor.runExtractor(targetFile, saveFile);
				collectLogger.loggingCollectDetailLog("[Modbus Parsing] SUCCESS. file=[" + fileInfoMap.get("absolutePath") + "]");
			} catch (Exception e) {
				collectLogger.loggingCollectDetailLog("[Modbus Parsing] FAIL. file=[" + fileInfoMap.get("absolutePath") + "]", e);
				saveFile.delete();
				continue;
			}

			long saveFileSize = super.save(saveFile, targetFileName, collectLogEncoding, false);
			if(saveFileSize > -1) {
				collectLogger.loggingCollectDetailLog("[GET file] SUCCESS. file=[" + fileInfoMap.get("absolutePath") + "], size=[" + saveFileSize + "byte]");
				if("Y".equals(deleteYn)) {
					targetFile.delete();
					collectLogger.loggingCollectDetailLog("[GET file] Delete file. file=[" + fileInfoMap.get("absolutePath") + "]");
				}

				// MetaFile 정보 갱신
				if (saveFileSize > -1 && metaFile != null) {
					FileWriter writer = null;
					try {
						writer = new FileWriter(metaFile);
						writer.write(fileInfoMap.get("absolutePath") + "|" + fileInfoMap.get("lastModifiedDate"));
						writer.flush();
					} catch (Exception e) {
						collectLogger.loggingCollectDetailLog("[Exception]", e);
					} finally {
						if(writer != null) try { writer.close(); } catch(IOException e) {}
					}
				}
			} else {
				// TODO: 실패한 파일 처리
				collectLogger.loggingCollectDetailLog("[GET file] FAIL. file=[" + fileInfoMap.get("absolutePath") + "]");
			}
		}
		collectLogger.loggingCollectDetailLog("[GET file] END");
	}

	/**
	 * MetaFile을 반환. 없을경우 생성
	 * @return MetaFile
	 */
	private File getMetaFile() throws IOException {

		File metaFileDirectory = new File(ConfigLoader.getInstance().get(Config.COLLECT_METAINFO_BASE_DIR) + File.separator + logpolicyId);
		if (!metaFileDirectory.exists()) metaFileDirectory.mkdirs();

		File metaFile = new File(metaFileDirectory  + File.separator + dataSourceId + "file_scan_meta.info");

		if (metaFile == null || !metaFile.exists()) {
			if (metaFile.createNewFile()) {
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
		if(metaFile == null) return null;

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
	 * @param currentDir 현재 디렉토리
	 * @param fileNamePattern 스캔 대상 파일명 패턴
	 * @param metaInfoMap 파일 절대경로, 파일 수정시간 등 메타정보
	 * @param compareType 파일 비교 타입(1:파일명(절대경로), 2:파일수정시간)
	 * @throws IOException
	 */
	private void getSaveFileList(File currentDir, String fileNamePattern, Map<String, String> metaInfoMap, int compareType) throws IOException {

		File[] fileList = null;
		if(Strings.isNullOrEmpty(fileNamePattern)) {
			fileList = currentDir.listFiles();
		} else {
			fileList = currentDir.listFiles(new LogFilenameFilter(fileNamePattern));
		}

		if(fileList == null) return;

		FileComparator comparator = new FileComparator(FileComparator.SORT_TYPE_ASC, compareType);
		for (File file : fileList) {
			if(file.isDirectory()) {
				getSaveFileList(file, fileNamePattern, metaInfoMap, compareType);
			} else {
        		// 1. 메타정보 비교
        		// 2. 저장 대상 목록에 추가
    			HashMap<String, String> fileInfoMap = new HashMap<String, String>();
				fileInfoMap.put("absolutePath", currentDir + File.separator + file.getName());
				fileInfoMap.put("lastModifiedDate", String.valueOf(file.lastModified()));

				if(metaInfoMap != null) {
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

	/**
	 * 정규표현식으로 파일명 패턴 비교
	 */
	public class LogFilenameFilter implements FilenameFilter {

		private String filenamePattern;
		
		public LogFilenameFilter(String filenamePattern) {
			this.filenamePattern = filenamePattern;
		}
		
		public boolean accept(File dir, String name) {
			
			File f = new File(dir, name);

			if (f.isDirectory()) {
				return true;
			} else {
				return Pattern.matches(filenamePattern, name);
			}
		}
	}

	/**
	 * 가장 최근 파일 제외(아직 쓰기가 진행되고 있는 파일일 수도 있기 때문)
	 * 마지막 파일은 제외하고 전송한다
	 */
	private void sortFileList(int compareType) {
		if(saveFilePathList.size() > 0) {
			Collections.sort(saveFilePathList, new FileComparator(FileComparator.SORT_TYPE_ASC, compareType));

			// 수집대상 파일중 마지막 file을 제외하고 수집한다.
			// 마지막 file에 write중일 수도 있음을 고려
			if (!ConfigLoader.getInstance().getBoolean(ConfigLoader.COLLECT_LAST_MODIFIED_FILE)) {
				saveFilePathList.remove(saveFilePathList.size()-1);
			}
		}
	}

	/**
	 * 파일패턴 유효성 체크
	 * @param patternStr
	 * @return
	 */
	private boolean checkPattern(String patternStr) {
		if(patternStr == null) {
			return false;
		}
		
		try {
			Pattern.compile(patternStr);
			return true;
		} catch (Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
			return false;
		}
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
						rslt = fileInfoMap2.get(subCompareKey).compareTo(fileInfoMap1.get(subCompareKey));
					}
					break;
			}
			return rslt;
		}
	}
}
