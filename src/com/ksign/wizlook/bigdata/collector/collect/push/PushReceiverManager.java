package com.ksign.wizlook.bigdata.collector.collect.push;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectAction;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.util.XmlParser;

/**
 * PushReceiver 관리 클래스
 * 수집방식이 Push 인 데이터수집유형의 구현체는 PushReceiver를 상속받아 구현한다.
 * @author byw
 */
public enum PushReceiverManager {
	INSTANCE;
	/** logger */
	private static final Logger logger = LogManager.getLogger();
	/** 현재 구동 중인 PushReceiver 를 담고 있는 Map */
	private static Map<String, PushReceiver> serverMap = new HashMap<String, PushReceiver>();

	/**
	 * PushReceiver 동작 상태 조회
	 * @param dataSourceId 데이터소스 아이디
	 * @return 동작상태
	 */
	public Map<String, Map<String, String>> getPushReceiverStatus(String dataSourceId) {
		Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();

		if(Strings.isNullOrEmpty(dataSourceId)) {
			for(String receiverId : serverMap.keySet()) {
				Map<String, String> receiverInfo = new HashMap<String, String>();
				receiverInfo.put("status", CollectAction.RUNNING.toString());
				resultMap.put(receiverId, receiverInfo);
			}
		} else {
			Map<String, String> receiverInfo = new HashMap<String, String>();
			if(serverMap.containsKey(dataSourceId)) {
				receiverInfo.put("status", CollectAction.RUNNING.toString());
				resultMap.put(dataSourceId, receiverInfo);
			}
		}
		return resultMap;
	}

	/**
	 * PushReceiver의 구동/중지
	 * @param dataSourceMapList pushReceiver 설정 정보
	 * @return 구동/중지 결과
	 * @throws CollectException
	 */
	public boolean manageReceiver(List<Map<String, String>> dataSourceMapList) throws CollectException {
		try {
			for(Map<String, String> dataSourceMap : dataSourceMapList) {
				if(CollectorCode.CollectAction.RUNNING.toString().equals(dataSourceMap.get("receiverAction"))) {
					startReceiver(dataSourceMap);
				} else if(CollectorCode.CollectAction.STOP.toString().equals(dataSourceMap.get("receiverAction"))) {
					stopReceiver(dataSourceMap.get("dataSourceId"));
				}
			}
		} catch(CollectException e) {
			for(Map<String, String> dataSourceMap : dataSourceMapList) {
				try {
					stopReceiver(dataSourceMap.get("dataSourceId"));
				} catch(CollectException e1) {
					logger.error(this.getClass().getSimpleName(), e);
				}
			}
			throw e;
		}
		return true;
	}

	/**
	 * PushReceiver 구동
	 * @param dataSourceMap pushReceiver 설정 정보
	 * @return 구동 결과
	 * @throws CollectException
	 */
	private synchronized boolean startReceiver(Map<String, String> dataSourceMap) throws CollectException {

		try {
			if( serverMap.containsKey(dataSourceMap.get("dataSourceId")) ) {
				if(!stopReceiver(dataSourceMap.get("dataSourceId"))) {
					return false;
				}
			}

			int port = 0;
			try {
				port = Integer.parseInt(dataSourceMap.get("receiverPort"));
			} catch (NumberFormatException e) {
				logger.error("Port=[" + port + "] is not number.");
				throw new CollectException(CollectorCode.Code.NOT_AVAILABLE_PORT);
			}

			if(isNotAvailablePort(port)) {
				logger.error("Port=[" + port + "] is not available.");
				throw new CollectException(CollectorCode.Code.NOT_AVAILABLE_PORT);
			}

			PushReceiver pushReceiver = PushReceiverFactory.getNewInstance(dataSourceMap.get("implClassPath"));

 			pushReceiver.init(dataSourceMap.get("logpolicyId"),
 							  dataSourceMap.get("dataSourceId"),
 							  dataSourceMap.get("allowHosts"),
 							  port,
 							  dataSourceMap.get("collectLogEncoding"),
							  new XmlParser().getXmlDataToMap(dataSourceMap.get("receiverConfigData")));

			boolean startResult = pushReceiver.start();
			if(startResult) {
				serverMap.put(dataSourceMap.get("dataSourceId"), pushReceiver);
			} else {
				logger.error("PushReceiver Start Fail.. dataSourceId=[" + dataSourceMap.get("dataSourceId")+ "], Port=[" + port + "]");
				return false;
			}

			logger.info("PushReceiver Start... DataSourceId=[" + dataSourceMap.get("dataSourceId") + "], Port=[" + port + "], Type=[" + serverMap.get(dataSourceMap.get("dataSourceId")).getClass().getSimpleName() + "]");
		} catch (CollectException e) {
			throw e;
		} catch (Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_START_RECEIVER);
		}

		return true;
	}

	/**
	 * PushReceier 중지
	 * @param dataSourceId 중지할 데이터소스아이디
	 * @return 중지 결과
	 * @throws CollectException
	 */
	private synchronized boolean stopReceiver(String dataSourceId) throws CollectException {

		PushReceiver pushReceiver = serverMap.get(dataSourceId);
		if(pushReceiver == null) return true;

		boolean result = false;
		try {
			result = pushReceiver.stop();
		} catch(Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_STOP_RECEIVER);
		}

		if(result) {
			serverMap.remove(dataSourceId);
			logger.info("PushReceiver Stop... dataSourceId=["+dataSourceId+ "], Port=["+pushReceiver.getPort()+"], Type=[" + pushReceiver.getClass().getSimpleName() + "]");
		}
		return result;
	}

	/**
	 * 모든 PushReceiver들의 임시 저장된 로그파일을 롤링
	 * @throws IOException
	 * @throws ParseException
	 * @throws CollectException
	 */
	public void rollingReceiveLogFile() throws IOException, ParseException, CollectException {
		for(PushReceiver receiver : serverMap.values()) {
			// 무조건 강제 롤링이 아님. 파일의 rolling주기가 지났지만 rolling이 되지 않은 파일만 롤링이 된다. ( forceRolling = false )
			receiver.rollingReceiveLogFile(false);
		}
	}

	/**
	 * 해당 port가 현재 사용 중인지 여부 체크
	 * @param port 확인 대상 포트
	 * @return
	 */
	private boolean isNotAvailablePort(int port) {
		ServerSocket tcpSocket = null;
		DatagramSocket udpSocket = null;

		try {          
			tcpSocket = new ServerSocket(port);
		} catch (IOException e) {
			return true;
		} finally {
			if(tcpSocket != null) try { tcpSocket.close(); } catch (IOException e) { }
		}

		try {          
			udpSocket = new DatagramSocket(port);
		} catch (IOException e) {
			return true;
		} finally {
			if(udpSocket != null) {
				udpSocket.close();
				// udp socket이 닫히는데 시간이 걸려서 sleep..
				try { Thread.sleep(100); } catch (InterruptedException e) { }
			}
		}
		return false;
	}
	
	public void destroy() {
		Iterator<Entry<String, PushReceiver>> iterator = serverMap.entrySet().iterator();
		while(iterator.hasNext()) {
			try {iterator.next().getValue().stop(); } catch (Exception e) { logger.error(this.getClass().getSimpleName(), e); }
		}
	}
	
	public static class PushReceiverFactory {
		public static PushReceiver getNewInstance(String classPath) throws CollectException {
			if(Strings.isNullOrEmpty(classPath)) throw new CollectException(CollectorCode.Code.NOT_FOUND_IMPL_CLASSPATH); 
			try {
				Class<?> cls = Class.forName(classPath);
				return (PushReceiver) cls.newInstance();
			} catch (ClassNotFoundException e) {
				logger.error(PushReceiverManager.class.getSimpleName(), e);
				throw new CollectException(CollectorCode.Code.NOT_FOUND_IMPL_CLASSPATH);
			} catch (InstantiationException | IllegalAccessException e) {
				logger.error(PushReceiverManager.class.getSimpleName(), e);
				throw new CollectException(CollectorCode.Code.INVALID_IMPL_CLASSPATH);
			}
		}
	}
}
