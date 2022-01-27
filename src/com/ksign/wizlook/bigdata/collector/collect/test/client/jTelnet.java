package com.ksign.wizlook.bigdata.collector.collect.test.client;

import thor.app.BasicTelnet;
import thor.app.VT100Telnet;
import thor.app.telnet;
import thor.net.TelnetURLConnection;

public class jTelnet  {
	public static void main(String[] args) {
		String[] test = {"10.20.170.160"};
//		telnet.main(test);
		VT100Telnet.main(test);
	}
}  