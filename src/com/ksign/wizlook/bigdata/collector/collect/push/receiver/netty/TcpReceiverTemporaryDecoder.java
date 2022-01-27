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
 * TcpReceiver의 connectionType이 'T'일 경우 동작한다.
 *  - 한개의 로그 데이터를 수신한 뒤 해당 connection은 close 된다. 
 * 각 connection 별 Decoder가 생성된다.
 * @author byw
 */
public class TcpReceiverTemporaryDecoder extends ByteToMessageDecoder {

	private final TcpReceiver tcpReceiver;
	private ByteBuf byteBuf;
	private boolean isSuccess = true;
	
	private File saveFile = null;
	private FileOutputStream outStream = null;
	private FileChannel fileChannel = null;

	/** logger */
	private final Logger logger = LogManager.getLogger();
	private CollectLogger collectLogger;
	private long startTime = 0;

	public TcpReceiverTemporaryDecoder(TcpReceiver tcpReceiver) {
		this.tcpReceiver = tcpReceiver;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {

		this.byteBuf = byteBuf;

		int readableBytesSize = byteBuf.readableBytes();
		if(byteBuf.readableBytes() < 1) return;

		try {

			if(saveFile == null) {
				// 저장할 file생성
				startTime = System.currentTimeMillis();
				saveFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator + UUID.randomUUID().toString());
				outStream = new FileOutputStream(saveFile);
				fileChannel = outStream.getChannel();

				// logging
				collectLogger = new CollectLogger(tcpReceiver.getLogpolicyId(), tcpReceiver.getDataSourceId());
				collectLogger.loggingCollectStart(startTime);
			}
			fileChannel.write(byteBuf.readBytes(readableBytesSize).nioBuffer());
		} catch(Exception e) {
			throw e;
		}
	}
	
	/**
	 * File에 저장 중 exception 발생 시 버퍼에 남은 내용을 마저 파일에 쓰고 해당 파일을 에러디렉토리로 이동
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		isSuccess = false;
		errorProcess();
		if(collectLogger != null) {
			collectLogger.loggingCollectDetailLog("[Exception]", cause);
			collectLogger.loggingCollectEnd(System.currentTimeMillis(), 0, CollectStatus.ERROR);
		}
		super.exceptionCaught(ctx, cause);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if(fileChannel != null) try { fileChannel.close(); } catch (IOException e) {}
		if(outStream != null) try { outStream.close(); } catch (IOException e) {}
		// 정상종료 시 파일 이동
		if(isSuccess && saveFile != null) { 
			long saveFileSize = tcpReceiver.save(saveFile);
			// logging
			collectLogger.loggingCollectEnd(System.currentTimeMillis(), saveFileSize, CollectStatus.SUCCESS);
			collectLogger = null;
		}
		super.channelInactive(ctx);
	}

	/**
	 * File에 저장 중 exception 발생 시 버퍼에 남은 내용을 마저 파일에 쓰고 해당 파일을 에러디렉토리로 이동
	 */
	private void errorProcess() {
		if(byteBuf != null && byteBuf.readableBytes() > 0 && fileChannel != null) {
			try { fileChannel.write(byteBuf.readBytes(byteBuf.readableBytes()).nioBuffer()); 
			} catch (Exception e) { logger.error(this.getClass().getSimpleName(), e); }
		}

		if(fileChannel != null) try { fileChannel.close(); } catch (IOException e) {}
		if(outStream != null) try { outStream.close(); } catch (IOException e) {}

		if(saveFile != null && saveFile.length() > 0) {
			tcpReceiver.saveErrorFile(saveFile);
		}
	}
}