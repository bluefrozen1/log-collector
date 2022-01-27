package com.ksign.wizlook.bigdata.collector.collect.test.client;

import java.net.InetAddress;
import java.util.Date;

import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.PDUv1;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.TransportMapping;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.IpAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TcpAddress;
import org.snmp4j.smi.TimeTicks;
import org.snmp4j.smi.TransportIpAddress;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultTcpTransportMapping;
import org.snmp4j.transport.DefaultUdpTransportMapping;

public class SnmpTrapClient {
	public static final String community = "public";

	// Sending Trap for sysLocation of RFC1213
	public static final String Oid = ".1.3.6.1.2.1.1.8";

	// IP of Local Host
	public static final String ipAddress = "127.0.0.1";
//	public static final String ipAddress = "10.20.170.160";

	// Ideally Port 162 should be used to send receive Trap, any other available
	// Port can be used
//	public static final int port = 161;
//	public static final int port = 162;
	public static final int port = 9100;
	
	public static final String protocol = "TCP";
//	public static final String protocol = "UDP";

	public static void main(String[] args) {
		SnmpTrapClient snmpTrapClient = new SnmpTrapClient();
		snmpTrapClient.sendTrap_Version1();
		snmpTrapClient.sendTrap_Version2();
		try {
			TransportMapping transport = null;
			if("TCP".equals(protocol)) {
				transport = new DefaultTcpTransportMapping();
			} else {
				transport = new DefaultUdpTransportMapping();
			}
			Snmp snmp = new Snmp(transport);
			transport.listen();
			System.out.println("Sending PDU");
			ResponseEvent response = snmp.send(getPDU(), getTarget());
			System.out.println("PDU sent");
			Thread.sleep(500);
			if (response != null) {
				System.out.println("GOT" + response.getError());
				System.out.println("GOT" + response.getResponse().getRequestID());
				System.out.println("GOT" + response.getError());
			}
		} catch (Exception exp) {
			System.out.println("Exception sending message" + exp);
		} catch (Error err) {
			System.out.println("Error sending message");
		}
		
	}
	/**
	 * This methods sends the V1 trap to the Localhost in port 162
	 */
	public void sendTrap_Version1() {
		try {
			
			TransportMapping transport = null;
			Address address = null;
			// Create Transport Mapping
			if("TCP".equals(protocol)) {
				transport = new DefaultTcpTransportMapping();
				address = new TcpAddress(ipAddress + "/" + port);
			} else {
				transport = new DefaultUdpTransportMapping();
				address = new UdpAddress(ipAddress + "/" + port);
			}
			transport.listen();
			
			// Create Target
			CommunityTarget cTarget = new CommunityTarget();
			cTarget.setCommunity(new OctetString(community));
			cTarget.setVersion(SnmpConstants.version1);
			cTarget.setAddress(address);
			cTarget.setTimeout(5000);
			cTarget.setRetries(2);

			PDUv1 pdu = new PDUv1();
			pdu.setType(PDU.V1TRAP);
			pdu.setEnterprise(new OID(Oid));
			pdu.setGenericTrap(PDUv1.ENTERPRISE_SPECIFIC);
			pdu.setSpecificTrap(1);
			pdu.setAgentAddress(new IpAddress(ipAddress));

			// Send the PDU
			Snmp snmp = new Snmp(transport);
			System.out.println("Sending V1 Trap... Check Wheather NMS is Listening or not? ");
			ResponseEvent event = snmp.send(pdu, cTarget);
			System.out.println("event = "+event);
			Thread.sleep(500);
			snmp.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This methods sends the V1 trap to the Localhost in port 162
	 */
	public void sendTrap_Version2() {
	
		try {
			
			TransportMapping transport = null;
			Address address = null;
			// Create Transport Mapping
			if("TCP".equals(protocol)) {
				transport = new DefaultTcpTransportMapping();
				address = new TcpAddress(ipAddress + "/" + port);
			} else {
				transport = new DefaultUdpTransportMapping();
				address = new UdpAddress(ipAddress + "/" + port);
			}

			// Create Target
			CommunityTarget cTarget = new CommunityTarget();
			cTarget.setCommunity(new OctetString(community));
			cTarget.setVersion(SnmpConstants.version2c);
			cTarget.setAddress(address);
			cTarget.setRetries(2);
			cTarget.setTimeout(5000);

			// Create PDU for V2
			PDU pdu = new PDU();

			// need to specify the system up time
			pdu.add(new VariableBinding(SnmpConstants.sysUpTime, new OctetString(new Date().toString())));
			pdu.add(new VariableBinding(SnmpConstants.snmpTrapOID, new OID(Oid)));
			pdu.add(new VariableBinding(SnmpConstants.snmpTrapAddress, new IpAddress(ipAddress)));

			pdu.add(new VariableBinding(new OID(Oid), new OctetString("Major")));
//			pdu.setType(PDU.NOTIFICATION);
			pdu.setType(PDU.TRAP);

			// Send the PDU
			Snmp snmp = new Snmp(transport);
			System.out.println("Sending V2 Trap... Check Wheather NMS is Listening or not? ");
			ResponseEvent event = snmp.send(pdu, cTarget);
			Thread.sleep(500);
			System.out.println("event = "+event);
			snmp.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static PDU getPDU() throws Exception {
		VariableBinding var;
		PDUv1 pdu = new PDUv1();
		pdu.setType(PDU.TRAP);
		try {

			var = new VariableBinding();
			var.setOid(new OID("1.3.6.1.4.1"));
			var.setVariable(new TimeTicks(12222L));
			pdu.add(var);
			var = new VariableBinding();
			var.setOid(new OID("1.3.6.1.4.1"));
			var.setVariable(new Integer32(99));
			pdu.add(var);

		} catch (Exception p) {
			p.printStackTrace();
		}
		return pdu;
	}
	
	
	private static Target getTarget() throws Exception {
		TransportIpAddress targetAddress = null;
		if("TCP".equals(protocol)) {
			targetAddress = new TcpAddress(InetAddress.getByName("127.0.0.1"), 163);
		} else {
			targetAddress = new UdpAddress(InetAddress.getByName("127.0.0.1"), 162);
		}
		CommunityTarget target = new CommunityTarget();
		target.setCommunity(new OctetString("public"));
		target.setAddress(targetAddress);
		target.setVersion(SnmpConstants.version2c);
		target.setTimeout(500L);
		target.setRetries(3);
		return target;
	}

}
