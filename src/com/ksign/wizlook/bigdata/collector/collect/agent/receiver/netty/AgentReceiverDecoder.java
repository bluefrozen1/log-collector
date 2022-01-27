package com.ksign.wizlook.bigdata.collector.collect.agent.receiver.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.UseYn;
import com.ksign.wizlook.bigdata.collector.collect.agent.AgentCollect;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectLogger;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.common.entity.CollectDataBean;
import com.ksign.wizlook.common.util.FileUtil;
import com.ksign.wizlook.common.util.HMACUtil;

/**
 * client로 부터 메시지를 수신할 때 거치는 클래스
 * 각 connection 별 Decoder가 생성된다.
 * @author byw
 */
public class AgentReceiverDecoder extends ByteToMessageDecoder {

	/** read charset */
	private Charset charset;
	/** client로 부터 수신된 데이터가 저장된 ByteBuf */
	private ByteBuf byteBuf;

	/** 헤더를 모두 읽었는지 여부 */
	private boolean headerReadComplete = false;
	/** 헤더 길이 */
	private int HEADER_LENGTH    			= 20;
	/** 정책아이디 */
	private int POLICY_ID_LENGTH 			= 6;
	/** 데이터소스아이디 */
	private int DATA_SOURCE_ID_LENGTH   	= 20;
	/** 전송일자 ( yyyyMMddHHmmss ) */
	private int SEND_DATE_LENGTH 			= 14;
	/** 압축 여부 ( Y / N ) */
	private int COMPRESS_YN_LENGTH 			= 1;
	/** 수집 로그 인코딩 길이 */
	private int COLLECT_LOG_ENCODING_LENGTH = 20;
	/** 로그파일의 파일명 길이를 나타내는 파라미터 길이 */
	private int FILE_NAME_LENGTH 			= 4;
	/** 로그파일 hmac 길이 */
	private int LOG_HMAC_LENGTH				= 44;

	private long readSize   = 0;
	private long bodyLength = 0;
	private long fileLength = 0;

	private File saveFile = null;
	private FileOutputStream outStream;
	private FileChannel fileChannel;
	private CollectDataBean bean;
	/** logger */
	private final Logger logger = LogManager.getLogger();
	private long startDate = 0;
	private CollectLogger collectLogger;

	public AgentReceiverDecoder(Charset charSet) {
		this.charset = charSet;
	}

	/**
	 * 메시지를 수신할 때 마다 호출되는 메소드
	 * 수신되는 data의 길이는 통신상황에 따라 상이하다.
	 * 메시지 전문 구성은 아래와 같다.
	 * header - 20 byte (String)  -> bodyLength
	 * body   - 6  byte (String)  -> logpolicyId
	 *        - 20 byte (String)  -> dataSourceId
	 *        - 14 byte (String)  -> sendDate
	 *        - 1  byte (String)  -> compressYn
	 *        - 44 byte (String)  -> logHmac
	 *        - 4  byte (String)  -> fileNameLength
	 *        - n  byte (String)  -> fileName (AbsolutePath)
	 *        - n  byte (byte[])  -> file (파일 또는 logData)
	 * @param ctx Netty ChannelHandlerContext
	 * @param byteBuf 수신된 data가 들어있는 Netty ByteBuf
	 * @param out Handler로 전달할 Object
	 */
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> out) throws Exception {

		this.byteBuf = byteBuf;

		// buffer read mark
		byteBuf.markReaderIndex();

		try {
			// 아직 header를 read하지 않았을 경우
			if(!headerReadComplete) {

				// channelbuffer에 들어있는 byte의 크기가 필수데이터보다 작을 경우 (header 20byte + policyId 6byte + dataSourceId 20byte + sendDate 14byte + compresYn 1byte + collectLogEncoding + logHmac 44byte + fileNameLength 4byte)
				if(byteBuf.readableBytes() < HEADER_LENGTH + POLICY_ID_LENGTH + DATA_SOURCE_ID_LENGTH + SEND_DATE_LENGTH + COMPRESS_YN_LENGTH + COLLECT_LOG_ENCODING_LENGTH + LOG_HMAC_LENGTH + FILE_NAME_LENGTH) {
					// return 을 하게 되면 channelbuffer데이터는 그대로 있고,
					// 추가적인 데이터를 client로 부터 수신받을 때 다시 decoder메소드가 호출된다.
					// 최소 109 byte 를 수신할 때 까지 return하여 대기한다.
					return;
				}

				// header read
				String bodyLengthStr = null;
				try {
					// header(bodyLength) parsing
					bodyLengthStr = new String(byteBuf.readBytes(HEADER_LENGTH).array(), charset).trim();
					bodyLength = Long.parseLong(bodyLengthStr);
					if(bodyLength < 1) {
						throw new IOException("Invalid BodyLength. value=[" + bodyLengthStr + "]");
					}
				} catch (Exception e) { 
					logger.error(this.getClass().getSimpleName(), e);
					throw new IOException("Invalid BodyLength. value=[" + bodyLengthStr + "]");
				}

				// body read
				// 6byte (String)  -> logpolicyId
				String logpolicyId = new String(byteBuf.readBytes(POLICY_ID_LENGTH).array(), charset).trim();
				// 20byte (String)  -> logpolicyId
				String dataSourceId = new String(byteBuf.readBytes(DATA_SOURCE_ID_LENGTH).array(), charset).trim();
				// 14byte (String) -> sendDate
				String sendDate = new String(byteBuf.readBytes(SEND_DATE_LENGTH).array(), charset).trim();
				// 1byte (String) -> compressYn
				String compressYn = new String(byteBuf.readBytes(COMPRESS_YN_LENGTH).array(), charset).trim();
				// 2byte (String) -> collectLogEncoding
				String collectLogEncoding = new String(byteBuf.readBytes(COLLECT_LOG_ENCODING_LENGTH).array(), charset).trim();
				// 44byte (String) -> logHmac
				String logHmac = new String(byteBuf.readBytes(LOG_HMAC_LENGTH).array(), charset).trim();
				// 4byte (String) -> fileNameLength
				int nameLength = Integer.parseInt(new String(byteBuf.readBytes(FILE_NAME_LENGTH).array(), charset).trim());

				// bodyLength 유효성 체크
				if(bodyLength < POLICY_ID_LENGTH + DATA_SOURCE_ID_LENGTH + SEND_DATE_LENGTH + COMPRESS_YN_LENGTH + COLLECT_LOG_ENCODING_LENGTH + LOG_HMAC_LENGTH + FILE_NAME_LENGTH + nameLength) {
					throw new IOException("Invalid BodyLength!");
				}

				// fileName을 아직 수신받지 못한 경우
				if(byteBuf.readableBytes() < nameLength) {
					byteBuf.resetReaderIndex();
					return;
				}

				// read FileName
				String logFileName = new String(byteBuf.readBytes(nameLength).array(), charset).trim();
				saveFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR)
												+ File.separator + logFileName + "_" + java.util.UUID.randomUUID().toString());

				if(!saveFile.getParentFile().exists()) {
					saveFile.getParentFile().mkdirs();
					saveFile.getParentFile().setWritable(true);
				}

				bean = new CollectDataBean(logpolicyId, dataSourceId, logFileName, saveFile, compressYn);
				bean.setSendDate(sendDate);
				bean.setLogHmac(logHmac);
				bean.setCollectLogEncoding(collectLogEncoding);

				fileLength = bodyLength - (POLICY_ID_LENGTH + DATA_SOURCE_ID_LENGTH + SEND_DATE_LENGTH + COMPRESS_YN_LENGTH + COLLECT_LOG_ENCODING_LENGTH + LOG_HMAC_LENGTH + FILE_NAME_LENGTH + nameLength);

				outStream = new FileOutputStream(saveFile);
				fileChannel = outStream.getChannel();
				headerReadComplete = true;
				startDate = System.currentTimeMillis();

				// logging
				collectLogger = new CollectLogger(logpolicyId, dataSourceId);
				collectLogger.loggingCollectStart(startDate);
			}

			// 한 connection으로 데이터를 계속 수신하기 위해
			// 모든 data를 읽으면 전역 파라미터들을 모두 초기화 해준다.
			int readableBytesSize = byteBuf.readableBytes();
			if(fileLength <= readSize + readableBytesSize) {

				long size = fileLength - readSize;

				fileLength = 0;
				bodyLength = 0;
				readSize = 0;
				headerReadComplete = false;

				fileChannel.write(byteBuf.readBytes((int)size).nioBuffer());
				fileChannel.close();
				outStream.close();

				// 수신한 데이터의 hmac 유효성 검사
				if(!HMACUtil.validate(bean.getLogFile(), bean.getLogHmac())) {
					throw new Exception("Receive logFile HMAC is invalid. file=[" + bean.getLogFile().getName() + "], receiveHMAC=[" + bean.getLogHmac() + "], fileHMAC=[" + HMACUtil.encode(bean.getLogFile()));
				}

				// 압축 여부에 따라 압축을 해제한다.
				if(UseYn.Y.toString().equals(bean.getCompressYn())) {
					collectLogger.loggingCollectDetailLog("[Decompress] Start Decompress logFile.");
					long decompressStartTime = System.currentTimeMillis();
					File decompressedFile = new File(bean.getLogFile() + "_decompress");
					FileUtil.decompressFile(bean.getLogFile(), decompressedFile);
					bean.setLogFile(decompressedFile);
					collectLogger.loggingCollectDetailLog("[Decompress] SUCCESS. fileSize=[" + decompressedFile.length() + "], elapsedTime=[" + (System.currentTimeMillis() - decompressStartTime) + "]");
				}
				AgentCollect agentCollect = new AgentCollect(bean.getLogpolicyId(), bean.getDataSourceId());
				long logFileSize = agentCollect.save(bean.getLogFile(), bean.getLogFile().getName(), bean.getCollectLogEncoding(), false);

				CollectStatus result = CollectStatus.ERROR;
				if(logFileSize > -1) result = CollectStatus.SUCCESS;

				collectLogger.loggingCollectEnd(System.currentTimeMillis(), logFileSize, result);

				collectLogger = null;
				fileChannel = null;
				outStream = null;
				saveFile = null;
				bean = null;

				// client에게 write를 할 경우 등록된 Encoder에서 추가적인 처리를 하여 전송한다.
				// TODO: return 메시지 구조 및 코드 정의 필요
				ctx.channel().writeAndFlush("SUCCESS");
				return;
			}

			readSize+=readableBytesSize;
			// Netty byteBuf에 들어있는 data를 fileChannel에 ByteBuffer형태로 바로 전달하여 write한다.
			fileChannel.write(byteBuf.readBytes(readableBytesSize).nioBuffer());

		} catch(Exception e) {
			byteBuf.resetReaderIndex();
			logger.error(this.getClass().getName(), e);
			throw e;
		}
	}

	/**
	 * client의 connection이 종료되었을 때 호출되는 메소드
	 * @param ctx Netty ChannelHandlerContext
	 */
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if(saveFile != null) errorProcess();
		super.channelInactive(ctx);
	}

	/**
	 * 통신 도중 Exception이 발생하였을 경우 호출되는 메소드
	 * @param ctx Netty ChannelHandlerContext
	 * @param cause 통신도중 발생된 Throwable 객체
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if(saveFile != null) errorProcess();

		// logging
		if(collectLogger != null) {
			long endTime = System.currentTimeMillis();
			collectLogger.loggingCollectDetailLog("[Exception]", cause);
			collectLogger.loggingCollectEnd(endTime, 0, CollectStatus.ERROR);
		}
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
			logger.error("Error occurred while AgentReceiver file receiving. File=[" + saveFile.getName() + "], Size=[" + saveFile.length() + "], elapsedTime=[" + (System.currentTimeMillis()-startDate) + "]");
			try {
				File file = new File(ConfigLoader.getInstance().get(Config.COLLECT_ERROR_BASE_DIR) + File.separator +
																	bean.getLogpolicyId() + File.separator +
																	bean.getDataSourceId() + File.separator +
																	saveFile.getName());
				if(!file.getParentFile().exists()) file.getParentFile().mkdirs();
				saveFile.renameTo(file);
			} catch (Exception e) { logger.error(this.getClass().getSimpleName(), e); }
		}
		byteBuf = null;
		saveFile = null;
	}
}