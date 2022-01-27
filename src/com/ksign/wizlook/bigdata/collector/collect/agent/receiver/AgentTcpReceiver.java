package com.ksign.wizlook.bigdata.collector.collect.agent.receiver;

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

import java.net.InetSocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.wizlook.bigdata.collector.collect.agent.receiver.netty.AgentReceiverChannelInitializer;

/**
 * Netty Server 구동 및 중지 클래스
 * @author byw
 */
public class AgentTcpReceiver {

	/** Bootstrap server channel */
	private Channel channel;
	/** Channel groups that the client is connected */
	private ChannelGroup channelGroup;
	/** Netty nio boss thread group */
	private EventLoopGroup bossGroup;
	/** Netty nio worker thread group */
	private EventLoopGroup workerGroup;
	/** logger */
	private final Logger logger = LogManager.getLogger();

	/**
	 * Agent Receiver 구동
	 * @param port 수신 port
	 * @return 구동 결과반환
	 */
	public boolean start(int port) {

		bossGroup = new NioEventLoopGroup();
		workerGroup = new NioEventLoopGroup(); // default : core count * 2; java option -Dio.netty.eventLoopThreads=30

		// channelGroup은 단순히 현재 connection 중인 client목록을 확인하기 위한 용도로 사용
		channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

		ServerBootstrap bootStrap = new ServerBootstrap();

		bootStrap.group(bossGroup, workerGroup)
				 .channel(NioServerSocketChannel.class)
				 .childOption(ChannelOption.SO_REUSEADDR, true)
				 .childOption(ChannelOption.SO_KEEPALIVE, true)
				 .childOption(ChannelOption.TCP_NODELAY, true)
				 .childOption(ChannelOption.SO_LINGER, 0) // server shutdown 시 client들이 접속 해 있어도 socket이 바로 close할 수 있도록 하기 위한 옵션
//				 .childOption(ChannelOption.SO_SNDBUF, 1024)
//				 .childOption(ChannelOption.SO_RCVBUF, 2048)
				 .childHandler(new AgentReceiverChannelInitializer(channelGroup));

		try {
			ChannelFuture future = bootStrap.bind(new InetSocketAddress(port)).sync();
			if(future.isSuccess()) {
				channel = future.channel();
				return true;
			}
		} catch (Exception e) {
			logger.error(this.getClass().getName(), e);
		}
		return false;
	}

	/**
	 * Agent Receiver 중지
	 * @return
	 */
	public boolean stop() {

		boolean result = false;

		// Server 에 연결되어 있는 client socket close
		if(channelGroup != null) {
			try { channelGroup.close().await(); } catch (InterruptedException e) { }
		}

		try {
			ChannelFuture future = channel.close().sync();
			if(future.isSuccess()) {
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
}