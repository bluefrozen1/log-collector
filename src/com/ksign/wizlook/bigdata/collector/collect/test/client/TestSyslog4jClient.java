package com.ksign.wizlook.bigdata.collector.collect.test.client;

import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogConfigIF;
import org.productivity.java.syslog4j.SyslogIF;
import org.productivity.java.syslog4j.impl.net.tcp.TCPNetSyslogConfig;
import org.productivity.java.syslog4j.impl.net.udp.UDPNetSyslogConfig;
import org.productivity.java.syslog4j.util.SyslogUtility;

public class TestSyslog4jClient {

//	private static String host = "10.20.170.160";
	private static String host = "localhost";
//	private static int port = 515;
	private static int port = 9200;
	
	private static String protocol = "TCP";
//	private static String protocol = "UDP";
	private static String name = "byw_test";
	
	public void init() {
		SyslogConfigIF config = null;
		SyslogIF client = null;
		
		if("TCP".equals(protocol)) {
			config = new TCPNetSyslogConfig(host, port);
		} else {
			config = new UDPNetSyslogConfig(host, port);
		}
		
		client = Syslog.createInstance(name, config);
		
		System.out.println("client start");
		
		client.warn("12313");
		SyslogUtility.sleep(500l);
		client.alert("Information 3123124");
		SyslogUtility.sleep(500l);
		client.info("Information 3123124");
		SyslogUtility.sleep(500l);
		client.flush();
		client.shutdown();
		
		System.out.println("client end");
	}
	
	
	public static void main(String[] args) {
		try {
			TestSyslog4jClient client = new TestSyslog4jClient();
			client.init();
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
}
