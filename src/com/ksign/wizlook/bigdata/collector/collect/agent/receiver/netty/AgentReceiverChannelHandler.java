package com.ksign.wizlook.bigdata.collector.collect.agent.receiver.netty;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 * Netty의 Event가 발생하였을 때 비동기로 호출되는 Method를 구현한 클래스
 * 아래 구현된 Event 외 추가적인 Event를 Override하여 구현할 수 있다
 * @author byw
 */
public class AgentReceiverChannelHandler extends ChannelInboundHandlerAdapter {

	/** connected client group */
	private ChannelGroup channelGroup;
	/** logger */
	private final Logger logger = LogManager.getLogger();

	/**
	 * 생성자
	 * @param channelGroup connected 된 client를 담아둘 channel group
	 */
	public AgentReceiverChannelHandler(ChannelGroup channelGroup) {
		this.channelGroup = channelGroup;
	}

	/**
	 * client의 connection이 완료되었을 때 호출되는 메소드
	 * channelGroup에 client를 추가한다.
	 * @param ctx Netty ChannelHandlerContext
	 */
	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		channelGroup.add(ctx.channel());
		InetSocketAddress sockAddress = (InetSocketAddress)ctx.channel().remoteAddress();
		logger.info("Connected Agent. Remote address=[" + sockAddress.getAddress().getHostAddress() + "]");
	}

	/**
	 * 통신 도중 client가 전송하는 메시지를 read하였을 때 호출되는 메소드
	 * @param ctx Netty ChannelHandlerContext
	 * @param msg 메시지는 등록된 Decoder에서 처리한 결과이다
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {}

	/**
	 * 통신 도중 Exception이 발생하였을 경우 호출되는 메소드
	 * @param ctx Netty ChannelHandlerContext
	 * @param cause 통신도중 발생된 Throwable 객체
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error(this.getClass().getName(), cause);
		ctx.channel().writeAndFlush(cause.getMessage()).addListener(ChannelFutureListener.CLOSE);
	}

	/**
	 * client의 connection이 종료되었을 때 호출되는 메소드
	 * @param ctx Netty ChannelHandlerContext
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		channelGroup.remove(ctx.channel());
	}
}