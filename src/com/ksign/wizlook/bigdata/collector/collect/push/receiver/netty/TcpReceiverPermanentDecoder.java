package com.ksign.wizlook.bigdata.collector.collect.push.receiver.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectLogger;
import com.ksign.wizlook.bigdata.collector.collect.push.receiver.TcpReceiver;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * client로 부터 메시지를 수신할 때 거치는 클래스
 * TcpReceiver의 connectionType이 'P'일 경우 동작한다.
 *  - Header ( 20byte ) 를 통해 bodyLength를 수신받은 뒤 body를 수신받는다.
 *  - 이 과정을 통해 한번의 Connection으로 로그파일을 계속 수신한다.  
 * @author byw
 */
public class TcpReceiverPermanentDecoder extends ByteToMessageDecoder {

	/** 해당 Decoder를 사용하는 TcpReceiver */
	private final TcpReceiver tcpReceiver;
	/** Netty ByteBuf */
	private ByteBuf byteBuf;

	/** Header read 완료 여부 */
	private boolean headerReadComplete;
	/** Header 길이 */
	private int HEADER_LENGTH = 20;

	/** read 길이 */
	private long readSize   = 0;
	/** body 길이 */
	private long bodyLength = 0;

	/** 저장 파일 */
	private File saveFile;
	/** 파일 저장 Stream */
	private FileOutputStream outStream;
	/** 파일 저장 Channel */
	private FileChannel fileChannel;

	/** logger */
	private final Logger logger = LogManager.getLogger();
	/** 수집이력 logger */
	private CollectLogger collectLogger;
	/** 수집 시작 시간 */
	private long startTime = 0;

	/**
	 * 각 connection 당 한개의 TcpReceiverPermanentDecoder가 생성
	 * @param tcpReceiver
	 */
	public TcpReceiverPermanentDecoder(TcpReceiver tcpReceiver) {
		this.tcpReceiver = tcpReceiver;
	}

	/**
	 * 메시지 전문 구성은 아래와 같다.
	 * header - 20 byte (string) -> bodyLength
	 * body   - bodyLength byte
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {

		this.byteBuf = byteBuf;

		try {
			// 아직 header를 read하지 않았을 경우
			if(!headerReadComplete) {

				// byteBuf에 들어있는 byte의 크기가 헤더 길이보다 작을 경우
				if(byteBuf.readableBytes() < HEADER_LENGTH) {
					// return 을 하게 되면 추가적인 데이터를 client로 부터 수신받을 때까지 대기하였다가 수신받으면 다시 decoder메소드가 호출
					return;
				}

				String bodyLengthStr = null;
				try {
					// header(bodyLength) parsing
					bodyLengthStr = new String(byteBuf.readBytes(HEADER_LENGTH).array()).trim();
					bodyLength = Long.parseLong(bodyLengthStr);
					if(bodyLength < 1) {
						throw new IOException("Invalid BodyLength. value=[" + bodyLengthStr + "]");
					}
				} catch (Exception e) { 
					logger.error(this.getClass().getSimpleName(), e);
					throw new IOException("Invalid BodyLength. value=[" + bodyLengthStr + "]");
				}

				// 저장할 file생성
				saveFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator + UUID.randomUUID().toString());
				outStream = new FileOutputStream(saveFile);
				fileChannel = outStream.getChannel();
				headerReadComplete = true;
				startTime = System.currentTimeMillis();

				// logging
				collectLogger = new CollectLogger(tcpReceiver.getLogpolicyId(), tcpReceiver.getDataSourceId());
				collectLogger.loggingCollectStart(startTime);
			}

			int readableBytesSize = byteBuf.readableBytes();
			if(byteBuf.readableBytes() < 1) return;

			// bodyLength 만큼 모두 read하였을 경우
			if(bodyLength <= readSize + readableBytesSize) {

				long size = bodyLength - readSize;

				bodyLength = 0;
				readSize = 0;
				headerReadComplete = false;

				fileChannel.write(byteBuf.readBytes((int)size).nioBuffer());
				fileChannel.close();
				outStream.close();
				fileChannel = null;
				outStream = null;
				File tmpFile = saveFile;
				saveFile = null;

				// 파일 이동
				long saveFileSize = tcpReceiver.save(tmpFile);

				// logging
				collectLogger.loggingCollectEnd(System.currentTimeMillis(), saveFileSize, CollectStatus.SUCCESS);
				collectLogger = null;
				return;
			}

			readSize += readableBytesSize;
			fileChannel.write(byteBuf.readBytes(readableBytesSize).nioBuffer());

		} catch(Exception e) {
			throw e;
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if(saveFile != null) errorProcess();
		super.channelInactive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if(collectLogger != null) {
			collectLogger.loggingCollectDetailLog("[Exception]", cause);
			collectLogger.loggingCollectEnd(System.currentTimeMillis(), 0, CollectStatus.ERROR);
		}
		if(saveFile != null) errorProcess();
		super.exceptionCaught(ctx, cause);
	}

	/**
	 * File에 저장 중 exception 발생 시 버퍼에 남은 내용을 마저 파일에 쓰고 해당 파일을 에러디렉토리로 이동
	 */
	private synchronized void errorProcess() {
		if(byteBuf != null && byteBuf.readableBytes() > 0 && fileChannel != null) {
			try { fileChannel.write(byteBuf.readBytes(byteBuf.readableBytes()).nioBuffer()); 
			} catch (Exception e) { logger.error(this.getClass().getSimpleName(), e); }
		}

		if(fileChannel != null) try { fileChannel.close(); } catch (IOException e) {}
		if(outStream != null) try { outStream.close(); } catch (IOException e) {}

		if(saveFile != null && saveFile.length() > 0) {
			tcpReceiver.saveErrorFile(saveFile);
		}
		byteBuf = null;
		saveFile = null;
	}
}