package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Vector;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.ScopedPDU;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.TransportIpAddress;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CommProtocol;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.common.util.DateUtil;

/**
 * 수집유형 SNMP 구현 클래스
 *  - SNMP Protocol을 통해 원격지의 데이터를 수집한다.
 * 이슈
 *  - 현재 snmp version 1, 2c 만 동작이 가능함 ( version 3을 사용하기 위해선 추가 개발 필요 )
 * @author byw
 */
public class SnmpJob extends PollJob {
	
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

		// SNMP version
		if(Strings.isNullOrEmpty(jobDataMap.get("version"))) return "version";
		// Community
		if(Strings.isNullOrEmpty(jobDataMap.get("community"))) return "community";
		// PDU type
		if(Strings.isNullOrEmpty(jobDataMap.get("pduType"))) return "pduType";
		// 통신 프로토콜
		if(Strings.isNullOrEmpty(jobDataMap.get("commProtocol"))) return "commProtocol";
//		String oid = jobConfigDataMap.get("oid");

		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobDataMap) throws Exception {

		// JOB 구동에 필요한 데이터(JOB SCHEDULE 최초 등록 시 입력된 데이터)
		String host = jobDataMap.get("dataServerHost");
		int port = Integer.parseInt(jobDataMap.get("port"));
		String oidStr = jobDataMap.get("oid");
		String community = jobDataMap.get("community");
		int version = getSnmpVersion(jobDataMap.get("version"));
		int pduType = getSnmpPduType(jobDataMap.get("pduType"));
		CommProtocol commProtocol = CommProtocol.valueOf(jobDataMap.get("commProtocol"));

		PDU pdu = null;
		Snmp snmp = null;

		try {
			// 1. Make Protocol Data Unit (version3은 ScopedPDU사용. version1은 PDU사용)
			if(version == SnmpConstants.version3) {
				pdu = new ScopedPDU();
			} else {
				pdu = new PDU();
			}

			if(!Strings.isNullOrEmpty(oidStr)) {
				String[] oidArr = oidStr.split(",");
				for(String oid : oidArr) {
					pdu.add(new VariableBinding(new OID(oid)));
				}
			}
			pdu.setType(pduType);

	        // 2. Make target 
	        CommunityTarget target = new CommunityTarget();

	        TransportIpAddress targetAddress = null;
	        TransportMapping<?> transportMapping = null;

	        if(CommProtocol.TCP == commProtocol) {
	        	targetAddress = new TcpAddress();
	        	transportMapping = new DefaultTcpTransportMapping();
	        } else if(CommProtocol.UDP == commProtocol) {
	        	targetAddress = new UdpAddress();
	        	transportMapping = new DefaultUdpTransportMapping();
	        } else {
	        	targetAddress = new UdpAddress();
	        	transportMapping = new DefaultUdpTransportMapping();
	        }

	        targetAddress.setInetAddress(InetAddress.getByName(host));
	        targetAddress.setPort(port);
	        target.setAddress(targetAddress);
	        target.setCommunity(new OctetString(community));
	        target.setVersion(version);

	        //3. Make SNMP Message
	        snmp = new Snmp(transportMapping);

	        //4. Send Message and Recieve Response
	        snmp.listen();
	        collectLogger.loggingCollectDetailLog("[SNMP send] host=[ " + host + "], port=[" + port + "], protocol=[" + commProtocol + "], version=[" + version + "]");
	        ResponseEvent response = snmp.send(pdu, target);

	        if (response.getResponse() == null) {
	        	collectLogger.loggingCollectDetailLog("[SNMP send] ERROR. response is null. error=[" + response.getError() + "]");
	        } else {
	        	PDU responsePdu = response.getResponse();
	        	collectLogger.loggingCollectDetailLog("[SNMP send] SUCCESS. requestId=[" + responsePdu.getRequestID() + "]");
	        	StringBuilder sb = new StringBuilder();
	        	sb.append("requestId=").append(responsePdu.getRequestID())
	        	  .append(",variableBindings=");
	        	Vector<? extends VariableBinding> variableBindings = responsePdu.getVariableBindings();
                for( int i = 0; i < variableBindings.size(); i++){
                	if(i > 0) sb.append("Ω");
                	sb.append(variableBindings.get(i).getOid()).append("=");
                	if(!variableBindings.get(i).isException()) {
                		sb.append(variableBindings.get(i).toValueString());
                	}
                }
                if(sb.length() > 0) {
                	long saveFileSize = super.save(sb.toString().getBytes(), "SNMP_" + DateUtil.getCurrentTimestampString(), collectLogEncoding, true);
                	collectLogger.loggingCollectDetailLog("[Save result] SUCCESS. save file size=[" + saveFileSize + "]");
                }
	        }
	        return true;
		} catch(Exception e) {
			throw e;
		} finally {
			if(snmp != null) {
				try { snmp.close(); } catch (IOException e) { logger.error(this.getClass().getSimpleName(), e); }
			}
		}
	}

	/**
	 * snmp4j PDU Object 조회
	 * @param pduType pduType string
	 * @return snmp4j PDU Object
	 */
	private int getSnmpPduType(String pduType) {
		if("GET".equals(pduType)) {
			return PDU.GET;
		} else if("GETNEXT".equals(pduType)) {
			return PDU.GETNEXT;
		} else if("GETBULK".equals(pduType)) {
			return PDU.GETBULK;
		}
		
		return 0;
	}

	/**
	 * snmp4j Version Object 조회
	 * @param version 버전 string
	 * @return snmp4j Version Object
	 */
	private int getSnmpVersion(String version) {
		if("VERSION1".equals(version)) {
			return SnmpConstants.version1;
		} else if("VERSION2".equals(version)) {
			return SnmpConstants.version2c;
		} else if("VERSION3".equals(version)) {
			return SnmpConstants.version3;
		}
		return 0;
	}
}