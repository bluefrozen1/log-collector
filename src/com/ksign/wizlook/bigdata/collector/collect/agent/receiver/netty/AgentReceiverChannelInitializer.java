package com.ksign.wizlook.bigdata.collector.collect.agent.receiver.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.CharsetUtil;

import java.io.FileInputStream;
import java.security.KeyStore;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.common.crypto.KsignCryptoUtils;
import com.ksign.wizlook.common.crypto.KsignInterfaceUtils;

/**
 * Netty 처리 Handler 등록 클래스
 * @author byw
 */
public class AgentReceiverChannelInitializer extends ChannelInitializer<SocketChannel>{

	/** connected client group */
	private ChannelGroup channelGroup;

	public AgentReceiverChannelInitializer(ChannelGroup channelGroup) {
		// channelGroup은 단순히 현재 connection 중인 client목록을 확인하기 위한 용도로 사용
		this.channelGroup = channelGroup;
	}

	/**
	 * Connection 이 신규로 체결될 때 아래 메소드가 호출되며 Handler 들을 등록한다.
	 */
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ConfigLoader config = ConfigLoader.getInstance();

		// For SSL
		if(config.getBoolean(Config.SSL_ENABLED)) {
			String sslProtocol = config.get(Config.SSL_PROTOCOL);
			String keystoreFile = config.get(Config.SSL_KEYSTORE_FILE);
			String keystorePass = KsignInterfaceUtils.dec(config.get(Config.SSL_KEYSTORE_PWD), KsignCryptoUtils.getInstance().getKey());

			// initialize
			KeyStore ks = KeyStore.getInstance("JKS");
			ks.load(new FileInputStream(keystoreFile), keystorePass.toCharArray());

			KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
			kmf.init(ks, keystorePass.toCharArray());

			SSLContext sslContext = SSLContext.getInstance(sslProtocol);
			sslContext.init(kmf.getKeyManagers(), null, null);

			// Create SSLEngine
			SSLEngine sslEngine = sslContext.createSSLEngine();
			sslEngine.setUseClientMode(false);
			ch.pipeline().addLast(new SslHandler(sslEngine));
		}

		// Data Send시 호출되는 Encoder class 등록
		ch.pipeline().addLast(new AgentReceiverEncoder(CharsetUtil.UTF_8));
		// Data Read시 호출되는 Decoder class 등록
		ch.pipeline().addLast(new AgentReceiverDecoder(CharsetUtil.UTF_8));
		// 비동기 이벤트 처리 시 호출되는 class 등록 ( connect, disconnect, exception, read, write등 Override하여 구현가능 )
		ch.pipeline().addLast(new AgentReceiverChannelHandler(channelGroup));
	}
}