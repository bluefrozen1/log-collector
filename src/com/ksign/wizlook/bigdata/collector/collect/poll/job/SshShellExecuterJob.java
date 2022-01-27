package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.StringTokenizer;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.common.util.DateUtil;

/**
 * 수집유형 SSH 구현 클래스
 *  - SSH Protocol로 원격지에 접속하여 shell/bat 파일을 실행하거나, 신규 생성하여 실행한 뒤 출력 결과를 수집한다.
 * 이슈
 *  - osX 테스트 필요
 * @author byw
 */
public class SshShellExecuterJob extends PollJob {
	/** ssh connection timeout millisecond */
	private int CONNECT_TIMEOUT = 10000;
	/** ssh key exchange time out millisecond */
	private int KEX_TIMEOUT = 0;
	
	
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
		String hostname = jobDataMap.get("dataServerHost");
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
		// os name
		String osName = jobDataMap.get("osName");
		String deleteYn = jobDataMap.get("deleteYn");
		String lineSeparator = System.getProperty("line.separator");

		Connection conn 	= null;
		Session session		= null;
		InputStream is		= null;
		InputStream errIs	= null;

		try {

			collectLogger.loggingCollectDetailLog("[SSH Connect] host=[" + hostname + "], port=[" + port + "]");
			conn = new Connection(hostname, port);
			conn.connect(null, CONNECT_TIMEOUT, KEX_TIMEOUT);
			collectLogger.loggingCollectDetailLog("[SSH Connect] SUCCESS");

			// 로그인
			collectLogger.loggingCollectDetailLog("[SSH Login] username=[" + username + "]");
			if (!conn.authenticateWithPassword(username, password)) {
				collectLogger.loggingCollectDetailLog("[SSH Login] Authentication failed");
				return false;
			}
			collectLogger.loggingCollectDetailLog("[SSH Login] SUCCESS");

			// ssh sheel / bat command
			StringBuilder command = new StringBuilder();

			//윈도우일 경우 ( .bat )
	        if (osName != null && osName.indexOf("Windows") > -1) {
	        	shellFilePath = "\""+shellFilePath+"\"";
	        	// 신규 .bat 파일을 생성하고 실행
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
		    		command.append("&&");

		    	// 기존에 존재하는 .bat 파일을 실행
				} else {
		        	command.append("cmd /c ");
				}

	    	//윈도우가 아닐 경우 ( .sh )
	        } else {
	        	command.append("echo \"").append(shellCommand.replace("\"", "\\\"").replace("$", "\\$").replace("`", "\\`")).append("\" > ").append(shellFilePath)
	        		   .append(" && chmod +x ").append(shellFilePath).append(";");
	        }

			command.append(shellFilePath);

			session = conn.openSession();
			collectLogger.loggingCollectDetailLog("[SSH ExecCommand] OS=[" + osName + "], command=[" + command.toString() + "]");
			session.execCommand(command.toString());

			// read error
			errIs = new StreamGobbler(session.getStderr());
			byte[] buffer = new byte[1024];
			int readLength = 0;
			StringBuilder errStr = new StringBuilder();
			while ( (readLength = errIs.read(buffer)) != -1 ) {
				errStr.append(new String(buffer, 0, readLength));
			}
			
			if(errStr.length() > 0) {
				collectLogger.loggingCollectDetailLog("[SSH ExecCommand] Error. cause=[" + errStr.toString() + "]");
				return false;
			}

			is = new StreamGobbler(session.getStdout());

			long saveFileSize = super.save(is, "SSH_SHELL_" + DateUtil.getCurrentTimestampString(), collectLogEncoding, true);
			collectLogger.loggingCollectDetailLog("[Save result] SUCCESS. save file size=[" + saveFileSize + "]");

			// 실행한 shell file 삭제
			if("Y".equals(deleteYn)) {
				command.setLength(0);
				//윈도우일 경우
		        if (osName != null && osName.indexOf("Windows") > -1) {
		        	command.append("cmd /c del /q ").append(shellFilePath);
		        } else {
		        	command.append("rm -f ").append(shellFilePath);
		        }

		        Session delSession = null;
		        try {
		        	collectLogger.loggingCollectDetailLog("[SSH Shell delete] command=[" + command + "]");
		        	delSession = conn.openSession(); 
		        	delSession.execCommand(command.toString());
		        	// 삭제 명령이 끝날때 까지 대기
		        	delSession.getStdout().read();
		        	delSession.getStdout().close();
		        	collectLogger.loggingCollectDetailLog("[SSH Shell delete] SUCCESS");
				} catch (IOException e) {
					collectLogger.loggingCollectDetailLog("[SSH Shell delete]", e);
				} finally {
					if(delSession != null) { delSession.close(); }
				}
        	}

			return true;
		} catch (Exception e) {
			throw e;
		} finally {
			if(session != null) { session.close(); }
			if(conn    != null) { conn.close();    }
			if(is      != null) { try { is.close();    } catch (IOException e) { logger.error(this.getClass().getSimpleName(), e); } }
			if(errIs   != null) { try { errIs.close(); } catch (IOException e) { logger.error(this.getClass().getSimpleName(), e); } }
		}
	}
}