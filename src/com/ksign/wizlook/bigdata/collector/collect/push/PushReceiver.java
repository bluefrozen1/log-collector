package com.ksign.wizlook.bigdata.collector.collect.push;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.AbstractCollect;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * PushReceiver 클래스
 * 수집방식이 Push 인 데이터수집유형의 구현체는 PushReceiver를 상속받아 구현한다.
 * @author byw
 */
public abstract class PushReceiver extends AbstractCollect {
	/** 허용 호스트 ( ,로 구분한 호스트 목록 ) */
	protected String allowHosts;
	/** 포트 */
	protected int port;
	/** 수집로그 인코딩 */
	protected String collectLogEncoding;

	/** 마지막 롤링 시간 ( Push로 수집하는 데이터는 File에 저장하고 있다가 일정시간마다 롤링한다.) */
	protected Date lastRollingDate = new Date();
	/** 파일 롤링 주기 */
	protected long fileRollingIntervalMillis = 1000L;
	/** 임시 저장 파일 */
	protected File bufferFile = null;
	/** write Lock */
	protected Object writeLock = new Object();
	/** 롤링파일 날짜 포맷 */
	private SimpleDateFormat fileRollingDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

	protected void init(String logpolicyId, String dataSourceId, String allowHosts, int port, String collectLogEncoding, Map<String, String> jobDataMap) {
		this.logpolicyId = logpolicyId;
		this.dataSourceId = dataSourceId;
		this.allowHosts = allowHosts;
		this.port = port;
		this.collectLogEncoding = collectLogEncoding;
		this.fileRollingIntervalMillis = ConfigLoader.getInstance().getLong(Config.PUSH_RECEIVE_BUFFER_SAVE_INTERVAL);
	}

	/**
	 * remoteAddress가 허용된 address인지 체크
	 * @param remoteAddress
	 * @return
	 * @throws SocketException
	 */
	public boolean isValidClientHost(InetSocketAddress remoteAddress) throws SocketException {
		// 1번이 false일 경우 2번 수행. 2번이 false일 경우 3번 수행

		// 1. 원격지의 address와 allowHosts를 비교
		// 2. 원격지가 loopBack이라면, 127.0.0.1외 실제 network설정에 있는 IP도 조회하여 비교
		// 3. allowHosts가 domain명일수도 있으니, 해당 domain의 IP를 조회하여 비교
		if(remoteAddress == null) throw new SocketException("cannot be null remote address.");
		if(Strings.isNullOrEmpty(allowHosts)) {
			return true;
		} else {
			String remoteHostAddress = remoteAddress.getAddress().getHostAddress();

			// 원격지의 address와 allowHosts를 비교
			String[] allowHostArr = allowHosts.split(",");
			for(String allowHost : allowHostArr) {
				if(Strings.isNullOrEmpty(allowHost)) continue;
				if(allowHost.equals(remoteHostAddress)) {
					return true;
				}
			}

//			// hostName을 가져오는 속도가 느리기 때문에 IP비교 결과가 false일 경우 실행
//			// 속도가 느려서 일단 사용안함 ( 테스트시 대략 4초 걸림.. )
//			String remoteHostName = remoteAddress.getAddress().getHostName();
//			// 원격지의 address와 allowHosts를 비교
//			for(String allowHost : allowHostArr) {
//				if(Strings.isNullOrEmpty(allowHost)) continue;
//				if(allowHost.equals(remoteHostName)) {
//					return true;
//				}
//			}

			// 유효성체크 패스를 못하였고, isLoopback일 경우
			// 자신의 실제 ip를 조회하여 한번 더 체크 (127.0.0.1이 아닌 실제 ip)
			if(remoteAddress.getAddress().isLoopbackAddress()) {
				List<String> localAddressList = getLocalAddresses();
				for(String allowHost : allowHostArr) {
					if(Strings.isNullOrEmpty(allowHost)) continue;
					for(String localAddress : localAddressList) {
						if(allowHost.equals(localAddress) || allowHost.equals(localAddress)) {
							return true;
						}
					}
				}
			}

			for(String allowHost : allowHostArr) {
				if(Strings.isNullOrEmpty(allowHost)) continue;
				// allowHost에 domain명이 들어가 있을 경우 실제 아이피를 조회하여 비교
				try {
					InetAddress[] inetAddressArr = InetAddress.getAllByName(allowHost);
					if(inetAddressArr == null || inetAddressArr.length < 1) continue;
					for(InetAddress address : inetAddressArr) {
						if(address.getHostAddress().equals(remoteHostAddress)) {
							return true;
						}
					}
				} catch (Exception e) {
					logger.error(this.getClass().getSimpleName(), e);
				}
			}
		}
		return false;
	}

	private List<String> getLocalAddresses() throws SocketException {
		List<String> resultList = new ArrayList<String>();
		Enumeration<NetworkInterface> niEnum = NetworkInterface.getNetworkInterfaces();		

		while(niEnum.hasMoreElements()) {
			NetworkInterface ni = niEnum.nextElement();

			Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
			while(inetAddresses.hasMoreElements()) { 
				InetAddress ia = inetAddresses.nextElement();
				if (ia.getHostAddress() != null && ia.getHostAddress().indexOf(".") != -1) {
					if("127.0.0.1".equals(ia.getHostAddress())) continue;
					resultList.add(ia.getHostAddress());
				}
			}
		}
		return resultList;
	}

	/**
	 * 수집한 로그를 파일로 저장한다.
	 * @param log 수집한 로그 String
	 * @param fileName 저장할 로그 파일명
	 * @param charSet 수집한 로그의 캐릭터셋
	 * @return 저장 성공 여부
	 * @throws IOException
	 * @throws CollectException
	 */
	protected boolean save(String log, String fileName, String charSet) throws IOException, CollectException {
		long saveFileSize = super.save(log.getBytes(), fileName, charSet, true);
		return saveFileSize > -1;
	}

	/**
	 * 수집한 로그를 바로 저장하지않고 일정시간동안 모아두었다가 한번에 롤링하여 저장한다.
	 * PushReceive Traffic이 많을 때 사용한다.
	 * @param log 수집한 로그 String
	 * @return 저장 성공 여부
	 * @throws IOException
	 * @throws ParseException
	 * @throws CollectException
	 */
	protected boolean bufferSave(String log) throws IOException, ParseException, CollectException {
		synchronized(writeLock) {
			long saveFileSize = -1;
			FileWriter writer = null;
			try {
				if(bufferFile == null) bufferFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator + dataSourceId + "," + fileRollingDateFormat.format(new Date()));
				writer = new FileWriter(bufferFile, true);
				writer.write(log);
				writer.write(System.lineSeparator());
				writer.flush();
			} catch (IOException e) {
				throw e;
			} finally {
				try { writer.close(); } catch (IOException e) {}
			}

			long fileCreateDate = fileRollingDateFormat.parse(bufferFile.getName().split(",")[1]).getTime();
			long currentDate = new Date().getTime();
			if(currentDate - fileCreateDate >= fileRollingIntervalMillis) {
				saveFileSize = super.save(bufferFile, bufferFile.getName(), "UTF-8", true);
				bufferFile = null;
			}
			return saveFileSize > -1;
		}
	}

	/**
	 * 파일을 롤링하여 저장한다.
	 * @param forceRolling 강제 롤링 여부
	 * @throws IOException
	 * @throws ParseException
	 * @throws CollectException
	 */
	protected void rollingReceiveLogFile(boolean forceRolling) throws IOException, ParseException, CollectException {
		synchronized(writeLock) {
			if(bufferFile == null) return;
			if(!forceRolling) {
				long fileCreateDate = fileRollingDateFormat.parse(bufferFile.getName().split(",")[1]).getTime();
				long currentDate = new Date().getTime();
				if(currentDate - fileCreateDate < fileRollingIntervalMillis) return; 
			}
			super.save(bufferFile, bufferFile.getName(), "UTF-8", true);

			bufferFile = null;
		}
	}

	public String getLogpolicyId() {
		return logpolicyId;
	}

	public String getDataSourceId() {
		return dataSourceId;
	}

	public String getAllowHost() {
		return allowHosts;
	}

	public int getPort() {
		return port;
	}

	public String getCollectLogEncoding() {
		return collectLogEncoding;
	}
}