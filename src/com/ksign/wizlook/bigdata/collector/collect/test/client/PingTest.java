package com.ksign.wizlook.bigdata.collector.collect.test.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class PingTest {
//	String allowHosts = "127.0.0.1";
	String allowHosts = "naver.com";

	public static void main(String[] args) {
		PingTest ping = new PingTest();
		ping.pingToDataServer();
	}
	protected boolean pingToDataServer() {
		InputStream inputStream = null;
    	InputStream errorStream = null;
		BufferedReader bufferedStandardReader = null;
		BufferedReader bufferedErrorReader = null;
		Process process = null;
		String lineSeparator = System.lineSeparator();
		String osName = System.getProperty("os.name").toLowerCase();
		try {
			// ping count:1, timeout:3(sec)
			if(osName.startsWith("win")) {
				String[] commands = {"cmd", "/c", "ping -n 1 -w 3 " + allowHosts};
				process = Runtime.getRuntime().exec(commands);
			} else {
				process = Runtime.getRuntime().exec("ping -c 1 -w 3 " + allowHosts);
			}
			process.waitFor(4L, TimeUnit.SECONDS);

			errorStream = process.getErrorStream();
			bufferedErrorReader = new BufferedReader(new InputStreamReader(errorStream, Charset.forName(System.getProperty("sun.jnu.encoding"))));
			String outMsg = null;
			StringBuilder errorSB = new StringBuilder();
			while ((outMsg = bufferedErrorReader.readLine()) != null) {
				errorSB.append(outMsg).append(lineSeparator);
			}
			if(errorSB.length() > 0) {
				// 마지막에 추가된 lineSeparator제거
				errorSB.delete(errorSB.length() - lineSeparator.length(), errorSB.length());
				System.out.println("error : " + errorSB.toString());
			} else {
				inputStream = process.getInputStream();
				bufferedStandardReader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName(System.getProperty("sun.jnu.encoding"))));
				StringBuilder resultSB = new StringBuilder();
				while ((outMsg = bufferedStandardReader.readLine()) != null) {
					resultSB.append(outMsg).append(lineSeparator);
				}
				if(resultSB.length() > 0) {
					// 마지막에 추가된 lineSeparator제거
					resultSB.delete(resultSB.length() - lineSeparator.length(), resultSB.length());
					System.out.println("success : " + resultSB.toString());
				}
			}
			if(process.exitValue() == 0) return true;
		} catch(InterruptedException e) {
			e.printStackTrace();
		} catch(IOException e) {
			e.printStackTrace();
		} finally {
			if(process != null) { process.destroy(); }
			try { if (bufferedErrorReader != null) { bufferedErrorReader.close(); } } catch (Exception e) { e.printStackTrace(); };
			try { if (bufferedStandardReader != null) { bufferedStandardReader.close(); } } catch (Exception e) { e.printStackTrace();};
		}
		return false;
	}
}
