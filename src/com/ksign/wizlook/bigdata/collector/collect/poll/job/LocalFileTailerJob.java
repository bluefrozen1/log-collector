package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;

/**
 * 수집유형 TAIL ( Agentless ) 구현 클래스
 *  - 로컬에 존재하는 File을 Tail하여 수집한다.
 * @author byw
 */
public class LocalFileTailerJob extends PollJob {

	@Override
	protected String validateParameter(Map<String, String> jobConfigDataMap) {
		if (Strings.isNullOrEmpty(jobConfigDataMap.get("filePath"))) return "filePath";
		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobConfigDataMap) throws Exception {
		String targetFilePath = jobConfigDataMap.get("filePath");
		RandomAccessFile targetFile = null;
		try {

			collectLogger.loggingCollectDetailLog("[Read file] file=[" + targetFilePath + "]");
			try {
				targetFile = new RandomAccessFile(targetFilePath, "r");
			} catch(FileNotFoundException e) {
				collectLogger.loggingCollectDetailLog("[Read file] ERROR. Is not exists file. file=[" + targetFilePath + "]");
				return false;
			}

			// 파일의 크기
			long fileLength = targetFile.length();
			long readPosition = fileLength;

			// 이전에 읽었던 포인터가 존재할 경우
			if(!Strings.isNullOrEmpty(jobConfigDataMap.get("readPosition"))) {
				// 파일의 크기가 포인터 값보다 클 경우 파일 read
				if(fileLength > Long.parseLong(jobConfigDataMap.get("readPosition"))) {
					readPosition = Long.parseLong(jobConfigDataMap.get("readPosition"));
				}

				long readSize = fileLength - readPosition;
				targetFile.seek(readPosition);
				ByteBuffer byteBuffer = ByteBuffer.allocateDirect(2048);
				StringBuilder sb = new StringBuilder();
				boolean readComplete = false;
				while(true) {
					targetFile.getChannel().read(byteBuffer);
					byteBuffer.flip();
					if(readSize > byteBuffer.remaining()) {
						readSize -= byteBuffer.remaining();
					} else {
						readComplete = true;
					}
					byte[] tempByteArrBuffer = new byte[(int) Math.min(byteBuffer.remaining(), readSize)];
					byteBuffer.get(tempByteArrBuffer);
					byteBuffer.clear();
					if(Strings.isNullOrEmpty(collectLogEncoding)) {
						sb.append(new String(tempByteArrBuffer));
					} else {
						sb.append(new String(tempByteArrBuffer, collectLogEncoding));
					}
					if(readComplete) break;
				}

				if(isInterruptedJob()) return false;
				if(sb.length() > 0) {
					long saveFileSize = super.save(sb.toString().getBytes(), new File(targetFilePath).getName(), "UTF-8", false);
					collectLogger.loggingCollectDetailLog("[Save data] SUCCESS. data size=[" + saveFileSize + "byte]");
				}
			}
			jobConfigDataMap.put("readPosition", String.valueOf(fileLength));
			return true;
		} catch (Exception e) {
			throw e;
		} finally {
			if(targetFile != null) { try { targetFile.close(); } catch (IOException e) {} }
		}
	}
}