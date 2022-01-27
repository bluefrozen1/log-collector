package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.StringTokenizer;

import javax.naming.AuthenticationException;

import org.apache.commons.net.telnet.TelnetClient;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.common.util.DateUtil;

/**
 * 수집유형 TELNET 구현 클래스
 *  - TELNET Protocol을 통해 원격지에 접속하여 shell/bat 명령을 실행한 뒤, 출력 결과를 수집한다.
 * 이슈
 *  - Telnet의 경우 대화형 프로토콜이기 때문에 OS별로 응답 메시지가 달라서 구현이 어렵다.
 *  - 현재 이 구현체는 프로토타입이며, 실제 사용이 필요할 경우 로직의 재 점검 및 구현이 필요
 * @author byw
 */
public class TelnetShellExecuterJob extends PollJob {

	private TelnetClient telnet = new TelnetClient();
	private InputStream in = null;
	private PrintStream out = null;
	private String prompt;
	private byte[] buffer = new byte[1024];

	public static void main(String[] args) {
		TelnetShellExecuterJob test = new TelnetShellExecuterJob();
		try {
			test.collect(null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected String validateParameter(Map<String, String> jobDataMap) {
		// 접속 host
		if(Strings.isNullOrEmpty(jobDataMap.get("dataServerHost"))) return "dataServerHost";
		// 접속 port
		if(Strings.isNullOrEmpty(jobDataMap.get("port"))) return "port";
		try {
			Integer.parseInt(jobDataMap.get("port"));
		} catch (Exception e) {
			return "port";
		}
		// 접속 user
		if(Strings.isNullOrEmpty(jobDataMap.get("userName"))) return "userName";
		// Shell 생성여부 (Y일 경우 신규 shell 생성, N일 경우 shell path에 있는 shell 실행)
		if(Strings.isNullOrEmpty(jobDataMap.get("createYn"))) return "createYn";
		// 실행 shell path, New Shell Script (둘 중 하나 필수)
		if(Strings.isNullOrEmpty(jobDataMap.get("shellFilePath"))
				&& Strings.isNullOrEmpty(jobDataMap.get("shellCommand"))) return "shellCommand or shellFilePath";
		
		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobDataMap) throws Exception {

		// 접속 host
		String host = jobDataMap.get("dataServerHost");
		// 접속 port
		int port = Integer.parseInt(jobDataMap.get("port"));
		// 접속 user
		String username = jobDataMap.get("userName");
		// 접속 password
		String password = jobDataMap.get("password"); 
		// Shell 생성여부 (Y일 경우 신규 shell 생성, N일 경우 shell path에 있는 shell 실행)
		String sshShellCreateYn	= jobDataMap.get("createYn");
		// 실행 shell path
		String shellFilePath = jobDataMap.get("shellFilePath");
		// New Shell Script
		String shellCommand	= jobDataMap.get("shellCommand");
		String deleteYn = jobDataMap.get("deleteYn");
		String lineSeparator = System.getProperty("line.separator");
		String osName = jobDataMap.get("osName");

		if("Window".equals(osName)) prompt = ">";
		else prompt = "] ";

		String logonExpectUsernameKey = "ogin: ";
		String logonExpectPasswordKey = "assword: ";

		try {
			// telnet 서버로의 접속
			collectLogger.loggingCollectDetailLog("[TELNET connect] host=[" + host + "], port=[" + port + "]");
			telnet.connect(host, port);
			collectLogger.loggingCollectDetailLog("[TELNET connect] SUCCESS");

			// read timeout 을 위한 설정
			telnet.setSoTimeout(5000);

			// 참조할 input, output stream 객체 획득
			in = telnet.getInputStream();
			out = new PrintStream(telnet.getOutputStream());

			try {
				// 사용자 로그온
				readUntil(logonExpectUsernameKey);
				write(username);
				readUntil(logonExpectPasswordKey);
				write(password);
			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
				throw new AuthenticationException("Authentication failed... ");
			}

			readUntil(prompt);

			StringBuilder command = new StringBuilder();

			// 윈도우일 경우 ( .bat )
	        if (osName != null && osName.indexOf("Windows") > -1) {
	        	shellFilePath = "\"" + shellFilePath + "\"";
	        	// 신규 shell 을 생성하고 실행
				if("Y".equals(sshShellCreateYn)) {
		        	StringTokenizer st = new StringTokenizer(shellCommand, lineSeparator);
		    		while (st.hasMoreTokens()) {
		    			if(command.length() == 0) {
		    				// 패일 새로 생성
		    				command.append("cmd /c ECHO ").append(st.nextToken()).append(">").append(shellFilePath);
		    			} else {
		    				// 이어쓰기
		    				command.append("&&ECHO ").append(st.nextToken()).append(">>").append(shellFilePath);
		    			}
		    		}
		    		command.append(" && ");
		    	
		    	// 기존에 존재하는 .bat 파일을 실행
				} else {
		        	command.append("cmd /c ");
				}

	    	// 윈도우가 아닐 경우 ( .sh )
	        } else {
	        	command.append("echo -e \"").append(shellCommand).append("\" > ").append(shellFilePath)
	        		   .append(" && chmod +x ").append(shellFilePath).append(";");
	        }
	        
			command.append(shellFilePath);

			write(command.toString());

			String result = readUntil(prompt);
			
			result = result.substring(0, result.lastIndexOf(lineSeparator));

			long saveFileSize = super.save(result.getBytes(), "TELNET_SHELL_" + DateUtil.getCurrentTimestampString(), collectLogEncoding, true);

			// 실행한 shell file 삭제
			if("Y".equals(deleteYn)) {
				command.setLength(0);
				//윈도우일 경우
		        if (osName != null && osName.indexOf("Windows") > -1) {
		        	command.append("cmd /c del /q ").append(shellFilePath);
		        } else {
		        	command.append("rm -f ").append(shellFilePath);
		        }
		        write(command.toString());
        	}
			return true;
		} catch (Exception e) {
			throw e;
		} finally {
			if(telnet != null) telnet.disconnect();
		}
	}

	/**
	 * 
	 * @param pattern
	 * @return java.lang.String
	 * @throws IOException 
	 */
	public String readUntil(String pattern) throws IOException {
		StringBuffer sb = new StringBuffer();

		int readLength = 0;
		while( ( readLength = in.read(buffer)) != -1 ) {
	    	String readStr = new String(buffer, 0, readLength);
	    	sb.append(readStr);
	    	if(!Strings.isNullOrEmpty(pattern) && readStr.endsWith(pattern) && in.available() == 0) {
	    		break;
	    	}
	    }
		
//		int readLength = 0;
//		while( (readLength = in.read(buffer)) != -1 ) {
//	    	String readStr = new String(buffer, 0, readLength);
//	    	sb.append(readStr);
//	    	if(!Strings.isNullOrEmpty(pattern) && readStr.endsWith(pattern) && in.available() == 0) {
//	    		break;
//	    	}
//	    }
	    return sb.toString();
	}

	/**
	 * 생성된 output stream에 value를 전송한다.
	 * 
	 * @param value
	 *            stream으로 전송할 String value
	 * @exception
	 */
	public void write(String value) throws Exception {
		out.println(value);
		out.flush();
	}

	/**
	 * Telnet 접속을 해제한다.
	 */
	public void disconnect() {
		try {
			telnet.disconnect();
		} catch (Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
		}
	}
}