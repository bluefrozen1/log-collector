package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;

import org.quartz.UnableToInterruptJobException;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;

/**
 * 수집유형 SHELL ( Agentless ) 구현 클래스
 *  - 로컬에 존재하는 shell을 실행하거나 신규 생성하여 실행한 결과를 수집한다.
 * @author byw
 */
public class LocalShellExecuterJob extends PollJob {
	private Thread currentThread;

	@Override
	protected String validateParameter(Map<String, String> jobConfigDataMap) {
		if (Strings.isNullOrEmpty(jobConfigDataMap.get("shellFilePath"))) return "shellFilePath";
		if (Strings.isNullOrEmpty(jobConfigDataMap.get("createYn"))) return "createYn";
		if("Y".equals(jobConfigDataMap.get("createYn"))) {
			if (Strings.isNullOrEmpty(jobConfigDataMap.get("shellCommand"))) return "shellCommand";
		} else if("N".equals(jobConfigDataMap.get("createYn"))) {
			if(!new File(jobConfigDataMap.get("shellFilePath")).exists()) return "shellFilePath";
		} else {
			return "createYn";
		}
		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobConfigDataMap) throws Exception {
		currentThread = Thread.currentThread();
		// Shell 생성여부 (Y일 경우 신규 shell 생성, N일 경우 shell path에 있는 shell 실행)
		String shellCreateYn = jobConfigDataMap.get("createYn");
		// 실행 shell path
		String shellFilePath = jobConfigDataMap.get("shellFilePath");
		// New Shell Script
		String shellCommand = jobConfigDataMap.get("shellCommand");
		// 실행 shell 삭제 여부
		String deleteYn = jobConfigDataMap.get("deleteYn");
		// OS 명
		String osName = System.getProperty("os.name");

		try {
			// 1. shell/bat 내용 검증 ( 필요? )
			// 2. shellCreateYn이 'Y'일 경우 shell/bat 파일 신규 생성
			// 3. shell/bat 실행
			// 4. shell/bat 출력결과 collector로 전송
			// 5. deleteYn이 'Y'일 경우 shell/bat파일 제거

			if("Y".equals(shellCreateYn)) {
				collectLogger.loggingCollectDetailLog("[Create file] file=[" + shellFilePath + "]");
        		createScriptFile(shellFilePath, shellCommand);
        		collectLogger.loggingCollectDetailLog("[Create file] SUCCESS");
        	}

			collectLogger.loggingCollectDetailLog("[ExecuteCommand] osName=[" + osName + "], command=[" + shellCommand + "]");
			String commandResult = executeCommand(osName, shellFilePath);

			long saveLogSize = 0;
			if(commandResult != null) {
				saveLogSize = super.save(commandResult.getBytes(), new File(jobConfigDataMap.get("shellFilePath")).getName(), "UTF-8", false);
				collectLogger.loggingCollectDetailLog("[Save data] SUCCESS. data size=[" + saveLogSize + "byte]");
			} else {
				collectLogger.loggingCollectDetailLog("[Save data] SUCCESS. data size=[" + saveLogSize + "byte]");
			}

			return saveLogSize > -1;
		} catch (Exception e) {
			throw e;
		} finally {
			// 생성하고 실행한 shell / batch 파일 삭제
			if("Y".equals(deleteYn)) {
				File shellFile = new File(shellFilePath);
				if(shellFile != null && shellFile.exists()) {
					boolean result = shellFile.delete();
					collectLogger.loggingCollectDetailLog("[Delete file] file=[" + shellFile.getAbsolutePath() + "], result=[" + (result ? "SUCCESS" : "FALSE") + "]");
				}
			}
		}
	}

	/**
	 * shell Script 파일 생성
	 * @param shellFilePath 파일 경로
	 * @param shellCommand 명령
	 * @throws IOException
	 */
	private void createScriptFile(String shellFilePath, String shellCommand) throws IOException {
		FileWriter writer = null;
		try {
			File shellFile = new File(shellFilePath);
			if(shellFile != null) {
				writer = new FileWriter(shellFilePath);
				writer.write(shellCommand);
				writer.flush();
			}
		} catch (IOException e) {
			throw e;
		} finally {
			if(writer != null) try { writer.close(); } catch (IOException e) {}
		}
	}

	/**
	 * Shell / bat 파일 실행
	 * @param osName os명
	 * @param shellFilePath 파일 경로
	 * @return 실행 결과
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private String executeCommand(String osName, String shellFilePath) throws InterruptedException, IOException {
		InputStream inputStream = null;
    	InputStream errorStream = null;
		BufferedReader bufferedStandardReader = null;
		BufferedReader bufferedErrorReader = null;
		Process process = null;

		try {
			// 윈도우일 경우 ( .bat )
	        if (osName != null && osName.indexOf("Windows") > -1) {
	        	String[] commands = {"cmd", "/c", "\"" + shellFilePath + "\""};
	    		process = Runtime.getRuntime().exec(commands);
	    		// TODO : timeout 설정
		        process.waitFor();

	    	// 윈도우가 아닐 경우 ( .sh )
	        } else {
	        	Runtime.getRuntime().exec("chmod +x " + shellFilePath).waitFor();
	        	process = Runtime.getRuntime().exec(shellFilePath);
	        	// TODO : timeout 설정
		        process.waitFor();
			}

			errorStream = process.getErrorStream();
			String collectFileEncoding = super.collectLogEncoding;
			if(Strings.isNullOrEmpty(collectFileEncoding)) collectFileEncoding = System.getProperty("sun.jnu.encoding");

			bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream, Charset.forName(collectFileEncoding)));

			String outMsg = null;
			StringBuilder errorSB = new StringBuilder();
			while ((outMsg = bufferedErrorReader.readLine()) != null) {
				errorSB.append(outMsg).append(System.lineSeparator());
			}
			if(errorSB.length() > 0) {
				// 마지막에 추가된 lineSeparator제거
				errorSB.delete(errorSB.length() - System.lineSeparator().length(), errorSB.length());
				collectLogger.loggingCollectDetailLog("[ExecuteCommand] ERROR=[" + errorSB.toString() + "]");
			} else {
				inputStream = process.getInputStream();
				bufferedStandardReader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName(collectFileEncoding)));
				StringBuilder resultSB = new StringBuilder();
				while ((outMsg = bufferedStandardReader.readLine()) != null) {
					resultSB.append(outMsg).append(System.lineSeparator());
				}
				if(resultSB.length() > 0) {
					// 마지막에 추가된 lineSeparator제거
					resultSB.delete(resultSB.length() - System.lineSeparator().length(), resultSB.length());
					return resultSB.toString(); 
				}
			}
		} finally {
			if(process != null) { process.destroy(); }
			try { if (bufferedErrorReader != null) { bufferedErrorReader.close(); } } catch (Exception e) { logger.error(this.getClass().getSimpleName(), e);};
			try { if (bufferedStandardReader != null) { bufferedStandardReader.close(); } } catch (Exception e) { logger.error(this.getClass().getSimpleName(), e);};
		}
		return null;
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		super.interrupt();
		currentThread.interrupt();
	}
}