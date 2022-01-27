package com.ksign.wizlook.bigdata.collector.collect.agent.receiver.netty;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * client로 메시지를 전송할 때 거치는 클래스
 * @author byw
 */
@Sharable
public class AgentReceiverEncoder extends MessageToMessageEncoder<CharSequence> {

	/** 전송되는 data charset */
    private final Charset charset;
    /** 전송 header length */
    private static final int HEADER_LENGTH = 20;

    public AgentReceiverEncoder() {
        this(Charset.defaultCharset());
    }

    public AgentReceiverEncoder(Charset charset) {
        if (charset == null) {
            throw new NullPointerException("charset");
        }
        this.charset = charset;
    }

    /**
     * 메시지를 전송할 때 마다 호출되는 메소드
	 * 메시지 전문 구성은 아래와 같다.
	 * header - 20 byte (String)  -> bodyLength
	 * body   - n  byte (String)  -> 처리 결과
     * @param ctx Netty ChannelHandlerContext
     * @param msg channel에 write요청된 메시지
     * @param out client로 전달할 Object
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, CharSequence msg, List<Object> out) throws Exception {

    	if (msg.length() == 0) {
            return;
        }

    	// 메시지의 앞부분에 bodylength를 붙인다.
    	long bodyLength = msg.toString().getBytes(charset).length;
    	msg = setPad(String.valueOf(bodyLength), HEADER_LENGTH, " ", "R") + msg;

        out.add(ByteBufUtil.encodeString(ctx.alloc(), CharBuffer.wrap(msg), charset));
    }
    
    /**
	 * 문자열을 패딩하여 반환
	 * @param str 입력문자열
	 * @param padLen 총자리수
	 * @param padChar 채워야할 문자
	 * @param padFlag 'L' LPAD, 'R' RPAD 구분
	 * @return String 패딩된 문자열 반환
	 */
	public static String setPad(String str, int padLen, String padChar, String padFlag) {
		if (str == null) {
			str = "";
		}
		
		if(str.length() > padLen)
			return str.substring(0, padLen);
		
		// LPAD
		if (padFlag.equals("L")) {
			while (str.length() < padLen)
				str = padChar + str;
		// RPAD
		} else if (padFlag.equals("R")) {
			while (str.length() < padLen)
				str = str + padChar;
		}

		return str;
	}
    
}
