package com.ksign.wizlook.bigdata.collector.collect.push.receiver;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Map;

import org.productivity.java.syslog4j.server.SyslogServer;
import org.productivity.java.syslog4j.server.SyslogServerConfigIF;
import org.productivity.java.syslog4j.server.SyslogServerEventIF;
import org.productivity.java.syslog4j.server.SyslogServerIF;
import org.productivity.java.syslog4j.server.SyslogServerSessionlessEventHandlerIF;
import org.productivity.java.syslog4j.server.impl.net.tcp.TCPNetSyslogServerConfig;
import org.productivity.java.syslog4j.server.impl.net.udp.UDPNetSyslogServerConfig;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CommProtocol;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectLogger;
import com.ksign.wizlook.bigdata.collector.collect.push.PushReceiver;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.common.util.DateUtil;

/**
 * 수집유형 Syslog 구현 클래스
 * @author byw
 */
public class Syslog4jReceiver extends PushReceiver implements SyslogServerSessionlessEventHandlerIF {
	private static final long serialVersionUID = 1L;
	/** Syslog Server */
	private SyslogServerIF server = null;
	/** 통신프로토콜 ( TCP, UDP ) */
	private CommProtocol commProtocol;

	@Override
	public void init(String logpolicyId, String dataSourceId, String allowHosts, int port, String collectLogEncoding, Map<String, String> jobDataMap) {

		super.init(logpolicyId, dataSourceId, allowHosts, port, collectLogEncoding, jobDataMap);

		this.commProtocol = CommProtocol.valueOf(jobDataMap.get("commProtocol"));
	}

	/**
	 * Syslog가 수신될 경우 호출되는 메소드
	 * @param syslogServer 로그 수신한 syslog Server
	 * @param socketAddress client socketAddress
	 * @param event syslog event 정보
	 */
	@Override
	public void event(SyslogServerIF syslogServer, SocketAddress socketAddress, SyslogServerEventIF event) {
		// 수집이력 저장 logger
		CollectLogger collectLogger = new CollectLogger(logpolicyId, dataSourceId);
		// 로그 수집 시작 이력 저장
		collectLogger.loggingCollectStart(System.currentTimeMillis());

		CollectStatus result = CollectStatus.ERROR;
		long logSize = 0;
		try {
			try {
				// 허용 host여부 체크
				if(!super.isValidClientHost((InetSocketAddress)socketAddress)) {
					collectLogger.loggingCollectDetailLog("[Check Host] Is not allow Host. Refuse connection. Address=[" + ((InetSocketAddress)socketAddress).getAddress().getHostAddress() + "]"); 
					return;
				}
			} catch (SocketException e) {
				collectLogger.loggingCollectDetailLog("[Exception]", e);
				return;
			}

			logSize = event.getRaw().length;
			try {
				// 수집로그 인코딩에 따라 로그를 저장
				String receiveMessage = null;
				if(Strings.isNullOrEmpty(collectLogEncoding) || "UTF-8".equals(collectLogEncoding)) {
					receiveMessage = new String(event.getRaw());
				} else {
					receiveMessage = new String(event.getRaw(), collectLogEncoding);
				}

				// 설정에 따라 buffer save 사용
				// buffer save를 사용할 경우 temp file에 기록하다가 일정 시간이 지나면 rolling하여 저장한다
				if(ConfigLoader.getInstance().getBoolean(Config.PUSH_RECEIVE_BUFFER_SAVE_ENABLED)) {
					if(server != null) super.bufferSave(receiveMessage);
				} else {
					super.save(receiveMessage, "SYSLOG_" + DateUtil.getCurrentTimestampString(), "UTF-8");
				}
				result = CollectStatus.SUCCESS;
			} catch (Exception e) {
				collectLogger.loggingCollectDetailLog("[Exception]", e);
			}
		} finally {
			// 로그 수집 완료 이력 저장
			collectLogger.loggingCollectEnd(System.currentTimeMillis(), logSize, result);
		}
	}

	@Override
	public void exception(SyslogServerIF syslogServer, SocketAddress socketAddress, Exception exception) {
		logger.error(this.getClass().getSimpleName(), exception);
	}

	@Override
	public boolean start() throws Exception {
		
		SyslogServerConfigIF config = null;
		
		if(CommProtocol.TCP == commProtocol) {
			config = new TCPNetSyslogServerConfig(port);
			((TCPNetSyslogServerConfig) config).setTimeout(10000);
			((TCPNetSyslogServerConfig) config).setMaxActiveSockets(1000);
			
		} else if (CommProtocol.UDP == commProtocol) {
			config = new UDPNetSyslogServerConfig(port);
		} else {
			config = new UDPNetSyslogServerConfig(port);
		}
		
		config.addEventHandler(this);
		config.setUseDaemonThread(true);
		config.setCharSet("UTF-8");
		
		server = SyslogServer.createThreadedInstance(dataSourceId, config);
		if(server != null && server.getThread() != null) {
			return true;
		}
		return false;
	}
	
	@Override
	public boolean stop() {
		try {
			SyslogServer.destroyInstance(server);
			server = null;
			if(ConfigLoader.getInstance().getBoolean(Config.PUSH_RECEIVE_BUFFER_SAVE_ENABLED)) {
				super.rollingReceiveLogFile(true);
			}
			return true;
		} catch (Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
		}
		return false;
	}

	@Override
	public void initialize(SyslogServerIF syslogServer) {
	}

	@Override
	public void destroy(SyslogServerIF syslogServer) {
	}
}