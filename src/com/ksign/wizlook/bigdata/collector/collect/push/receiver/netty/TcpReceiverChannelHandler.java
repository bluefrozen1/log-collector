package com.ksign.wizlook.bigdata.collector.collect.push.receiver.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

import java.net.InetSocketAddress;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectLogger;
import com.ksign.wizlook.bigdata.collector.collect.push.receiver.TcpReceiver;

/**
 * Netty의 Event가 발생하였을 때 비동기로 호출되는 Method를 구현한 클래스
 * 아래 구현된 Event 외 추가적인 Event를 Override하여 구현할 수 있다
 * @author byw
 */
public class TcpReceiverChannelHandler extends ChannelInboundHandlerAdapter {

	/** connected client group */
	private ChannelGroup channelGroup;
	private TcpReceiver tcpReceiver;

	public TcpReceiverChannelHandler(ChannelGroup channelGroup, TcpReceiver tcpReceiver) {
		this.channelGroup = channelGroup;
		this.tcpReceiver = tcpReceiver;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		InetSocketAddress sockAddress = (InetSocketAddress)ctx.channel().remoteAddress();
		String remoteHostAddress = sockAddress.getAddress().getHostAddress();
		if(!tcpReceiver.isValidClientHost(sockAddress)) {
			CollectLogger collectLogger = new CollectLogger(tcpReceiver.getLogpolicyId(), tcpReceiver.getDataSourceId());
			collectLogger.loggingCollectStart(System.currentTimeMillis());
			collectLogger.loggingCollectDetailLog("[Check Host] Is not allow address. Refuse connection. Adress=[" + remoteHostAddress + "]");
			collectLogger.loggingCollectEnd(System.currentTimeMillis(), 0, CollectStatus.ERROR);
			ctx.close();
		}
		channelGroup.add(ctx.channel());
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.channel().close();
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		channelGroup.remove(ctx.channel());
	}
}