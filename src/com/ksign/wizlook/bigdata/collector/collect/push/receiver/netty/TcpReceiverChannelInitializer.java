package com.ksign.wizlook.bigdata.collector.collect.push.receiver.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;

import java.util.concurrent.TimeUnit;

import com.ksign.wizlook.bigdata.collector.collect.agent.receiver.netty.AgentReceiverDecoder;
import com.ksign.wizlook.bigdata.collector.collect.push.receiver.TcpReceiver;

/**
 * Netty 처리 Handler 등록 클래스
 * @author byw
 *
 */
public class TcpReceiverChannelInitializer extends ChannelInitializer<SocketChannel>{

	/** Connection 된 Client 그룹 */
	private ChannelGroup channelGroup;
	/** 해당 초기화 클래스를 사용하는 TcpReceiver */
	private TcpReceiver tcpReceiver;

	public TcpReceiverChannelInitializer(ChannelGroup channelGroup, TcpReceiver tcpReceiver) {
		this.channelGroup = channelGroup;
		this.tcpReceiver = tcpReceiver;
	}

	/**
	 * Connection 이 신규로 체결될 때 아래 메소드가 호출되며 Handler 들을 등록한다.
	 */
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		if(tcpReceiver.getReadTimeoutMillis() > 0) {
			ch.pipeline().addLast(new ReadTimeoutHandler(tcpReceiver.getReadTimeoutMillis(), TimeUnit.MILLISECONDS));
		}

		// Data Read시 호출되는 Decoder class 등록
		if(TcpReceiver.CONNECTION_TYPE_PERMANENT.equals(tcpReceiver.getConnectionType())) {
			ch.pipeline().addLast(new TcpReceiverPermanentDecoder(tcpReceiver));
		} else if(TcpReceiver.CONNECTION_TYPE_TEMPORARY.equals(tcpReceiver.getConnectionType())) {
			ch.pipeline().addLast(new TcpReceiverTemporaryDecoder(tcpReceiver));
		} else {
			throw new IllegalArgumentException("Is invalid connectionType. connectionType=[" + tcpReceiver.getConnectionType() + "]");
		}

		// 비동기 이벤트 처리 시 호출되는 class 등록 ( connect, disconnect, exception, read, write등 Override하여 구현가능 )
		ch.pipeline().addLast(new TcpReceiverChannelHandler(channelGroup, tcpReceiver));
	}
}