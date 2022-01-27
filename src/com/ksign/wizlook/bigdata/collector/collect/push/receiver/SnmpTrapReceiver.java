package com.ksign.wizlook.bigdata.collector.collect.push.receiver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.Map;
import java.util.Vector;

import org.snmp4j.CommandResponder;
import org.snmp4j.CommandResponderEvent;
import org.snmp4j.CommunityTarget;
import org.snmp4j.MessageDispatcher;
import org.snmp4j.MessageDispatcherImpl;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.mp.MPv1;
import org.snmp4j.mp.MPv2c;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.Priv3DES;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.MultiThreadedMessageDispatcher;
import org.snmp4j.util.ThreadPool;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CommProtocol;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectLogger;
import com.ksign.wizlook.bigdata.collector.collect.push.PushReceiver;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.common.util.DateUtil;

/**
 * 수집유형 Snmp Trap 구현 클래스 
 * @author byw
 */
public class SnmpTrapReceiver extends PushReceiver implements CommandResponder {

	private Snmp snmp = null;
	private TransportMapping<?> transport = null;
	private ThreadPool threadPool = null;
	private CommProtocol commProtocol;

	@Override
	public void init(String logpolicyId, String dataSourceId, String allowHosts, int port, String collectLogEncoding, Map<String, String> jobDataMap) {

		super.init(logpolicyId, dataSourceId, allowHosts, port, collectLogEncoding, jobDataMap);

		this.commProtocol = CommProtocol.valueOf(jobDataMap.get("commProtocol"));
	}

	/**
	 * Snmp Trap 로그를 수신하였을 경우 호출되는 메소드
	 * @param commandResponderEvent 수신된 snmp Trap 정보
	 */
	@Override
	public void processPdu(CommandResponderEvent commandResponderEvent) {
		// 로그 수집 이력 logger
		CollectLogger collectLogger = new CollectLogger(logpolicyId, dataSourceId);
		// 로그 수집 시작 이력 저장
		collectLogger.loggingCollectStart(System.currentTimeMillis());

		CollectStatus result = CollectStatus.ERROR;
		long logSize = 0;
		try {
			try {
				String remoeteHost = commandResponderEvent.getPeerAddress().toString().split("/")[0];
				int remoetePort = Integer.parseInt(commandResponderEvent.getPeerAddress().toString().split("/")[1]);
				InetSocketAddress socketAddress = new InetSocketAddress(remoeteHost, remoetePort);

				// 허용 host여부 체크
				if(!super.isValidClientHost(socketAddress)) {
					collectLogger.loggingCollectDetailLog("[Check Host] Is not allow Host. Refuse connection. Address=[" + ((InetSocketAddress)socketAddress).getAddress().getHostAddress()+ ")]");
					return;
				}
			} catch (SocketException e) {
				collectLogger.loggingCollectDetailLog("[Exception]", e);
				return;
			}

			PDU pdu = commandResponderEvent.getPDU();
			if(pdu == null) {
				collectLogger.loggingCollectDetailLog("[Exception] Snmp Trap Receive Error. PDU is null.");
				return;
			}

			StringBuilder logMsg = new StringBuilder();
			logMsg.append("pduType=").append(PDU.getTypeString(pdu.getType()))
				  .append(",peerAddress=").append(commandResponderEvent.getPeerAddress())
			  	  .append(",requestId=").append(pdu.getRequestID())
			  	  .append(",variableBindings=");

			Vector<? extends VariableBinding> variableBindings = pdu.getVariableBindings();
		    for( int i = 0; i < variableBindings.size(); i++){
		    	if(i > 0) logMsg.append("Ω");
		    	logMsg.append(variableBindings.get(i).getOid()).append("=");
		    	if(!variableBindings.get(i).isException()) {
		    		logMsg.append(variableBindings.get(i).toValueString());
		    	}
		    }

		    logSize = logMsg.toString().getBytes().length;
			try {
				// 설정에 따라 buffer save 사용
				// buffer save를 사용할 경우 temp file에 기록하다가 일정 시간이 지나면 rolling하여 저장한다
				if(ConfigLoader.getInstance().getBoolean(Config.PUSH_RECEIVE_BUFFER_SAVE_ENABLED)) {
					super.bufferSave(logMsg.toString());
				} else {
					super.save(logMsg.toString(), "SNMP_TRAP_" + DateUtil.getCurrentTimestampString(), "UTF-8");
				}
				result = CollectStatus.SUCCESS;
			} catch (IOException e) {
				collectLogger.loggingCollectDetailLog("[Exception]", e);
			} catch (Exception e) {
				collectLogger.loggingCollectDetailLog("[Exception]", e);
			}
		} finally {
			// 로그 수집 완료 이력 저장
			collectLogger.loggingCollectEnd(System.currentTimeMillis(), logSize, result);
		}
		
	}

	@Override
	public boolean start() throws Exception {

		if(CommProtocol.TCP == commProtocol) {
			transport = new DefaultTcpTransportMapping(new TcpAddress(port));
		} else if(CommProtocol.UDP == commProtocol) {
			transport = new DefaultUdpTransportMapping(new UdpAddress(port));
		} else {

		}

		threadPool = ThreadPool.create("DispatcherPool", 1);
		MessageDispatcher mDispatcher = new MultiThreadedMessageDispatcher(threadPool, new MessageDispatcherImpl());

		USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(MPv3.createLocalEngineID()), 0);
		usm.setEngineDiscoveryEnabled(true);

		// add message processing models
		mDispatcher.addMessageProcessingModel(new MPv1());
		mDispatcher.addMessageProcessingModel(new MPv2c());
		mDispatcher.addMessageProcessingModel(new MPv3(usm));

		// add all security protocols
		SecurityProtocols.getInstance().addDefaultProtocols();
		SecurityProtocols.getInstance().addPrivacyProtocol(new Priv3DES());

		// Create Target
		CommunityTarget target = new CommunityTarget();
		target.setCommunity(new OctetString("public"));

		snmp = new Snmp(mDispatcher, transport);
		snmp.addCommandResponder(this);

		snmp.listen();

		return true;
	}

	@Override
	public boolean stop() {

		if(snmp != null) {
			try { snmp.close(); } catch (IOException e) { logger.error(this.getClass().getSimpleName(), e); }
		}

		if(threadPool != null) {
			threadPool.cancel();
		}
		if(ConfigLoader.getInstance().getBoolean(Config.PUSH_RECEIVE_BUFFER_SAVE_ENABLED)) {
			try { super.rollingReceiveLogFile(true); } catch (Exception e) { }
		}
		return true;
	}
}
