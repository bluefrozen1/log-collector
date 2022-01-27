package com.ksign.wizlook.bigdata.collector.collect.push.receiver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.collect.push.PushReceiver;
import com.ksign.wizlook.bigdata.collector.collect.push.receiver.netty.TcpReceiverChannelInitializer;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * 수집유형 TCP 구현 클래스
 * @author byw
 */
public class TcpReceiver extends PushReceiver {

	/** 영구적 Connection 유지 */
	public static final String CONNECTION_TYPE_PERMANENT = "P";
	/** 로그파일 수신 한건 당 한 Connection 유지 */
	public static final String CONNECTION_TYPE_TEMPORARY = "T";

	/** bootstrap server channel */
	private Channel channel;
	/** Channel groups that the client is connected */
	private ChannelGroup channelGroup;
	/** Netty nio boss thread group */
	private EventLoopGroup bossGroup;
	/** Netty nio worker thread group */
	private EventLoopGroup workerGroup;
	/** Connection type ( P / T ) */
	private String connectionType = CONNECTION_TYPE_PERMANENT;
	/** worker thread count */
	private int workerThreadCount = 1;
	/** read timeout millis */
	private long readTimeoutMillis = 0;
	/** logger */
	private final Logger logger = LogManager.getLogger();

	@Override
	public void init(String logpolicyId, String dataSourceId, String allowHosts, int port, String fromEncoding, Map<String, String> jobDataMap) {

		super.init(logpolicyId, dataSourceId, allowHosts, port, fromEncoding, jobDataMap);

		if(!Strings.isNullOrEmpty(jobDataMap.get("connectionType"))) {
			this.connectionType = jobDataMap.get("connectionType");
		}
		if(!Strings.isNullOrEmpty(jobDataMap.get("workerThreadCount"))) {
			this.workerThreadCount = Integer.parseInt(jobDataMap.get("workerThreadCount"));
		}
		if(!Strings.isNullOrEmpty(jobDataMap.get("readTimeoutMillis"))) {
			this.readTimeoutMillis = Long.parseLong(jobDataMap.get("readTimeoutMillis")); 
		}
	}

	@Override
	public boolean start() {

		bossGroup = new NioEventLoopGroup();
		if(workerThreadCount > 0) {
			workerGroup = new NioEventLoopGroup(workerThreadCount);
		} else {
			workerGroup = new NioEventLoopGroup(); // default : core count * 2; java option -Dio.netty.eventLoopThreads=30
		}

		channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

		ServerBootstrap bootStrap = new ServerBootstrap();

		bootStrap.group(bossGroup, workerGroup)
		   		 .channel(NioServerSocketChannel.class)
		   		 .childOption(ChannelOption.SO_REUSEADDR, true)
		   		 .childOption(ChannelOption.SO_KEEPALIVE, true)
				 .childOption(ChannelOption.TCP_NODELAY, true)
				 .childOption(ChannelOption.SO_LINGER, 1)
//				 .childOption(ChannelOption.SO_SNDBUF, 1024)
//				 .childOption(ChannelOption.SO_RCVBUF, 4096)
				 .childHandler(new TcpReceiverChannelInitializer(channelGroup, this));

		try {
			ChannelFuture future = bootStrap.bind(new InetSocketAddress(port)).sync();
			if(future.isSuccess()) {
				channel = future.channel();
				return true;
			}
		} catch (Exception e) {
			logger.error(this.getClass().getName(), e);
			if(bossGroup != null) bossGroup.shutdownGracefully();
			if(workerGroup != null) workerGroup.shutdownGracefully();
		}
		return false;
	}

	@Override
	public boolean stop() {

		boolean result = false;

		// Server 에 연결되어 있는 client socket close
		if(channelGroup != null) {
			try { channelGroup.close().await(); } catch (InterruptedException e) { }
		}

		try {
			ChannelFuture future = channel.close().sync();
			if(future.isSuccess()) {
				logger.info("AgentReceiver destroyed..");
				result = true;
			}
		} catch (InterruptedException e) {
			logger.error(this.getClass().getSimpleName(), e);
		}

		if(bossGroup != null && !bossGroup.isShutdown()) {
			bossGroup.shutdownGracefully();
		}

		if(workerGroup != null && !workerGroup.isShutdown()) {
			workerGroup.shutdownGracefully();
		}
		return result;
	}

	/**
	 * 수신하여 저장한 로그파일을 engine에게 전송하기 위한 디렉토리에 저장
	 * @param saveFile
	 * @return
	 * @throws IOException
	 * @throws CollectException
	 */
	public long save(File saveFile) throws IOException, CollectException {
		return super.save(saveFile, saveFile.getName(), collectLogEncoding, false);
	}

	/**
	 * 수신 도중 에러가 날 경우 COLLECT_ERROR_BASE_DIR에 수신받은 부분까지의 파일을 저장
	 * @param saveFile 에러나기 전까지 수신된 로그 파일
	 * @return 저장 성공 여부
	 */
	public boolean saveErrorFile(File saveFile) {
		File file = new File(ConfigLoader.getInstance().get(Config.COLLECT_ERROR_BASE_DIR) + File.separator +
							 logpolicyId + File.separator +
							 dataSourceId + File.separator +
							 saveFile.getName());
		if(!file.getParentFile().exists()) file.getParentFile().mkdirs();
		return saveFile.renameTo(file);
	}

	public String getConnectionType() {
		return connectionType;
	}

	public long getReadTimeoutMillis() {
		return readTimeoutMillis;
	}
}