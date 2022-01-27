package com.ksign.wizlook.bigdata.collector.itf.avro;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectAction;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJobManager;
import com.ksign.wizlook.bigdata.collector.collect.push.PushReceiverManager;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.dao.history.CollectHistoryDAO;
import com.ksign.wizlook.common.AvroClient;
import com.ksign.wizlook.common.itf.WizLookInterface;
import com.ksign.wizlook.common.itf.message.Message;
import com.ksign.wizlook.common.itf.message.MessageException;

/**
 * Console <-> Collector Interface 구현 클래스
 * @author byw
 */
public enum CollectorInterfaceService {
	INSTANCE;
	/** avro server */
	private static Server server = null;
	/** netty nio thread factory */
	private static NioServerSocketChannelFactory serverChannelFactory = null;
	/** logger */
	private static final Logger logger = LogManager.getLogger();
	/** Console Success Code */
	private final String CONSOLE_CODE_SUCCESS = "WAC0000000";

	/**
	 * Start CollectorInterface server
	 * @throws Exception
	 */
	public void start() throws Exception {
		if( server == null ) {
			serverChannelFactory = new NioServerSocketChannelFactory(Executors.newFixedThreadPool(1), Executors.newFixedThreadPool(5));
			server = new NettyServer( new SpecificResponder(WizLookInterface.class, new WizLookInterfaceImple()),
									  new InetSocketAddress(ConfigLoader.getInstance().getInt("interface.port")),
									  serverChannelFactory);
		}
		server.start();
    }

	/**
	 * Stop CollectorInterface Server
	 */
	public void destroy() {
		if(server != null) {
			try { serverChannelFactory.releaseExternalResources(); } catch (Exception e) { }
			try { server.close(); } catch (Exception e) { }
		}
	}

	public class WizLookInterfaceImple implements WizLookInterface {
		@Override
		public CharSequence call(CharSequence message) throws AvroRemoteException {
			String responseCode = null;
			try {
				// 유효성 체크
				if(message.length() == 0) throw new MessageException(CollectorCode.Code.NOT_FOUND_REQUEST_MESSAGE.getCode());

				// 수신 메시지 파싱
				Message requestMessage = null;
				try {
					requestMessage = new Message(message.toString());
				} catch (IOException e) {
					logger.error(this.getClass().getSimpleName(), e);
					throw new MessageException(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode());
				}

				logger.trace( "Request message:\n"+ requestMessage );

				// 펑션명 추출
				String function = requestMessage.getFunction();
				if (Strings.isNullOrEmpty(function)) throw new MessageException(CollectorCode.Code.NOT_FOUND_FUNCTION_NAME.getCode());

				Object responseContent = null;
				try {
					// 펑션명에 따른 요청 메소드 실행
					responseContent = CollectorInterfaceService.class.getMethod(function, Message.class).invoke(this, requestMessage);
				} catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
					logger.error(this.getClass().getSimpleName(), e);
					throw new MessageException(CollectorCode.Code.INVALID_FUNCTION_NAME.getCode());
				} 

				try {
					// 결과 메시지 전달
					String response = requestMessage.buildResponseMessage(new ObjectMapper().writeValueAsString(responseContent));
					logger.trace( "Response message:\n"+ response );
					return response;
				} catch (IOException e) {
					logger.error(this.getClass().getSimpleName(), e);
					throw new MessageException(CollectorCode.Code.INVALID_RESPONSE_MESSAGE.getCode());
				}

			} catch (MessageException e) {
				logger.error(this.getClass().getSimpleName(), e);
				responseCode = e.getErrorCode();
			} catch (Exception e) {
				logger.error(this.getClass().getSimpleName(), e);
				responseCode = CollectorCode.Code.UNKNOWN_ERROR.getCode();
			}
			return Message.makeResponseMessage(null, responseCode, null);
		}
	}


	/********** Server Function START **********/
	/*******************************************/

	/**
	 * 동작중인 Poll / Push 데이터소스 상태 조회
	 * @param message Request Message
	 * @return Response Message
	 */
	public static Object getDataSourceStatus(Message message) {
		Map<String, String> requestContentMap = null;
		try {
			requestContentMap = new ObjectMapper().readValue( message.getContent(), new TypeReference<Map<String, String>>(){} );
		} catch( IOException e ) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode());
		}

		Map<String, Map<String, String>> pollJobMap = null;
		Map<String, Map<String, String>> pushReceiverMap = null;

		try {
//			String version = requestContentMap.get("version");
			String dataSourceId = requestContentMap.get("dataSourceId");

			// 데이터소스아이디만으로는 해당 dataSource가 poll인지 push인지 구분할 수 없기 때문에 양쪽 모두 조회를 한다.
			// poll 상태조회
			pollJobMap = PollJobManager.INSTANCE.getJobStatus(dataSourceId);
			// push 상태조회
			pushReceiverMap = PushReceiverManager.INSTANCE.getPushReceiverStatus(dataSourceId);

			pollJobMap.putAll(pushReceiverMap);

			if(!Strings.isNullOrEmpty(dataSourceId) && pollJobMap.get(dataSourceId) == null) {
				Map<String, String> dataSourceMap = new HashMap<String, String>();
				dataSourceMap.put("status", CollectAction.STOP.toString());
				pollJobMap.put(dataSourceId, dataSourceMap);
			}

			message.setResponseCode(CollectorCode.Code.SUCCESS.getCode());
		} catch (CollectException e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(e.getErrorCode());
		} catch (Exception e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.UNKNOWN_ERROR.getCode());
		}

		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("version", "1.0.0");
		if(pollJobMap != null) resultMap.put("result", pollJobMap);

		return resultMap;
	}

	/**
	 * Poll 데이터수집 시작/중지 관리
	 * @param message Request Message
	 * @return Response Message
	 */
	public static Object managePollJob(Message message) {
		Map<String, Object> requestContentMap = null;
		try {
			requestContentMap = new ObjectMapper().readValue( message.getContent(), new TypeReference<Map<String, Object>>(){} );
		} catch( IOException e ) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode());
		}

		try {
//			String version = requestContentMap.get("version");
			@SuppressWarnings("unchecked")
			List<Map<String, String>> dataSourceMapList = (List<Map<String, String>>)requestContentMap.get("dataSourceInfo");
			if(dataSourceMapList != null) {
				PollJobManager.INSTANCE.manageJob(dataSourceMapList);
			}
			message.setResponseCode(CollectorCode.Code.SUCCESS.getCode());
		} catch (CollectException e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(e.getErrorCode());
		} catch (Exception e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.UNKNOWN_ERROR.getCode());
		}

		Map<String, String> resultMap = new HashMap<String, String>();
		resultMap.put("version", "1.0.0");
		return resultMap;
	}

	/**
	 * Push 데이터수집 시작/중지 관리
	 * @param message Request Message
	 * @return Response Message
	 */
	public static Object managePushReceiver(Message message) {
		Map<String, Object> requestContentMap = null;
		try {
			requestContentMap = new ObjectMapper().readValue( message.getContent(), new TypeReference<Map<String, Object>>(){} );
		} catch( IOException e ) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode());
		}

		try {
//			String version = requestContentMap.get("version");
			@SuppressWarnings("unchecked")
			List<Map<String, String>> dataSourceMapList = (List<Map<String, String>>)requestContentMap.get("dataSourceInfo");
			if(dataSourceMapList == null) throw new CollectException(CollectorCode.Code.INVALID_REQUEST_MESSAGE);

			PushReceiverManager.INSTANCE.manageReceiver(dataSourceMapList);

			message.setResponseCode(CollectorCode.Code.SUCCESS.getCode());
		} catch (CollectException e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(e.getErrorCode());
		} catch (Exception e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.UNKNOWN_ERROR.getCode());
		}

		Map<String, String> resultMap = new HashMap<String, String>();
		resultMap.put("version", "1.0.0");
		return resultMap;
	}

	/**
	 * 수집이력 조회
	 * @param message Request Message
	 * @return Response Message
	 */
	public static Object getCollectHistory(Message message) {
		Map<String, Object> requestContentMap = null;
		try {
			requestContentMap = new ObjectMapper().readValue( message.getContent(), new TypeReference<Map<String, Object>>(){} );
		} catch( IOException e ) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode());
		}

		List<Map<String, String>> historyList = null;
		long collectHistoryCount = 0;
		try {
//			String version = requestContentMap.get("version");
			String logpolicyId = (String)requestContentMap.get("logpolicyId");
			String dataSourceId = (String)requestContentMap.get("dataSourceId");
			String startDate = (String)requestContentMap.get("startDate");
			String endDate = (String)requestContentMap.get("endDate");
			String status = (String)requestContentMap.get("status");
			int limit = (Integer)requestContentMap.get("limit");
			int offset = (Integer)requestContentMap.get("offset");

			historyList = CollectHistoryDAO.INSTANCE.getCollectHistoryList(logpolicyId, dataSourceId, startDate, endDate, status, limit, offset);
			collectHistoryCount = CollectHistoryDAO.INSTANCE.getCollectHistoryCount(logpolicyId, dataSourceId, startDate, endDate, status);

			message.setResponseCode(CollectorCode.Code.SUCCESS.getCode());
		} catch (CollectException e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(e.getErrorCode());
		} catch (Exception e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.UNKNOWN_ERROR.getCode());
		}

		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("version", "1.0.0");
		resultMap.put("count", collectHistoryCount);
		resultMap.put("list", historyList == null ? Collections.EMPTY_LIST : historyList);
		return resultMap;
	}

	/**
	 * 수집 상세 이력 조회
	 * @param message Request Message
	 * @return Response Message
	 */
	public static Object getCollectHistoryDetail(Message message) {
		Map<String, String> requestContentMap = null;
		try {
			requestContentMap = new ObjectMapper().readValue( message.getContent(), new TypeReference<Map<String, String>>(){} );
		} catch( IOException e ) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode());
		}

		String detailLog = null;
		try {
//			String version = requestContentMap.get("version");
			String logpolicyId = requestContentMap.get("logpolicyId");
			String dataSourceId = requestContentMap.get("dataSourceId");
			String sessionId = requestContentMap.get("sessionId");

			detailLog = CollectHistoryDAO.INSTANCE.getCollectHistoryDetail(logpolicyId, dataSourceId, sessionId);

			message.setResponseCode(CollectorCode.Code.SUCCESS.getCode());

		} catch (CollectException e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(e.getErrorCode());
		} catch (Exception e) {
			logger.error(CollectorInterfaceService.class.getSimpleName(), e);
			message.setResponseCode(CollectorCode.Code.UNKNOWN_ERROR.getCode());
		}

		Map<String, String> resultMap = new HashMap<String, String>();
		resultMap.put("version", "1.0.0");
		resultMap.put("result", detailLog);
		return resultMap;
	}

	/*********** Server Function END ***********/
	/*******************************************/





	/*******************************************/
	/********** Client Function START **********/

	/**
	 * Console로 요청메시지를 보내고 결과를 반환 받는다.
	 * @param host Console host
	 * @param port Console port
	 * @param functionName 요청 function명
	 * @param content Request Message
	 * @return Response Message
	 * @throws InterfaceException
	 */
	private String requestConsole(String host, int port, String functionName, String content) throws InterfaceException {
		// 요청 메세지 생성
		String requestMessage = null;
		try {
			requestMessage = Message.makeRequestMessage(null, functionName, content);
		} catch(MessageException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new InterfaceException(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode(), e);
		}

		// 서버에 요청
		Map<String, Object> responseMessage = null;
		try {
			responseMessage = call(host, port, requestMessage);
		} catch(IOException e) {
			throw new InterfaceException(CollectorCode.Code.FAIL_REQUEST.getCode(), e);
		}
		if(responseMessage == null)
			throw new InterfaceException(CollectorCode.Code.INVALID_RESPONSE_MESSAGE.getCode());

		// 정상 종료가 아닐 경우 Exception 발생
		if( !CONSOLE_CODE_SUCCESS.equals(responseMessage.get("return")) )
			throw new InterfaceException((String)responseMessage.get("return"));

		return (String)responseMessage.get("content");
	}

	/**
	 * Console로 요청메시지를 보내고 결과를 반환 받는다.
	 * @param host Console host
	 * @param port Console port
	 * @param requestMessage json으로 파싱된 request Message
	 * @return Response Message
	 * @throws IOException
	 * @throws InterfaceException
	 */
	private Map<String, Object> call( String host, int port, String requestMessage ) throws IOException, InterfaceException {

		logger.trace( "Request message:\n"+ requestMessage );

		AvroClient client = null;
		CharSequence result = null;
		try {
			client = new AvroClient( host, port, 1000L );
			result = client.getProxy( WizLookInterface.class ).call( requestMessage );
		} finally {
			if( client != null ) client.close();
		}

		if( result == null || result.length() == 0 ) return null;

		// 응답 메세지 파싱
		Map<String, Object> responseMessageMap = null;
		try {
			responseMessageMap = new ObjectMapper().readValue( result.toString(), new TypeReference<Map<String, Object>>(){} );
		} catch(IOException e) {
			throw new InterfaceException(CollectorCode.Code.INVALID_RESPONSE_MESSAGE.getCode(), e);
		}
		if( responseMessageMap == null || responseMessageMap.isEmpty() )
			throw new InterfaceException(CollectorCode.Code.NOT_FOUND_RESPONSE_MESSAGE.getCode());
		else if( responseMessageMap.containsKey("result") )
			throw new InterfaceException(CollectorCode.Code.INVALID_RESPONSE_MESSAGE.getCode());

		logger.trace( "Response message:\n"+ responseMessageMap );

		return responseMessageMap;
	}

	/**
	 * 현재 동작해야할 dataSource 정보 요청
	 * @throws InterfaceException
	 */
	public void requestDataSource() throws InterfaceException {
		Map<String, String> contentMap = new HashMap<String, String>();
		contentMap.put("version", "1.0.0");
		String content;
		try {
			content = new ObjectMapper().writeValueAsString(contentMap);
		} catch (IOException e) {
			throw new InterfaceException(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode(), e);
		}

		requestConsole(ConfigLoader.getInstance().get(Config.CONSOLE_HOST),
					   ConfigLoader.getInstance().getInt(Config.CONSOLE_RPC_PORT),
					   "requestDataSource",
					   content);
	}

	/**
	 * 정책패턴 정보 등록/수정 요청
	 * @param logpolicyId 정책아이디
	 * @param patternId 패턴아이디
	 * @param pattern 패턴
	 * @throws InterfaceException
	 */
	public void registLogpolicyPattern(String logpolicyId, String patternId, String pattern) throws InterfaceException {
		Map<String, String> contentMap = new HashMap<String, String>();
		contentMap.put("version", "1.0.0");
		contentMap.put("logpolicyId", logpolicyId);
		contentMap.put("patternId", patternId);
		contentMap.put("pattern", pattern);

		String content = null;
		try {
			content = new ObjectMapper().writeValueAsString(contentMap);
		} catch (IOException e) {
			throw new InterfaceException(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode(), e);
		}

		requestConsole(ConfigLoader.getInstance().get(Config.CONSOLE_HOST),
					   ConfigLoader.getInstance().getInt(Config.CONSOLE_RPC_PORT),
					   "registLogpolicyPattern",
					   content);
	}

	/**
	 * 인덱싱 데이터 초기화 요청
	 * @param logpolicyId 정책아이디
	 * @throws InterfaceException
	 */
	public void initIndexData(String logpolicyId) throws InterfaceException {
		Map<String, String> contentMap = new HashMap<String, String>();
		contentMap.put("version", "1.0.0");
		contentMap.put("logpolicyId", logpolicyId);
		String content = null;
		try {
			content = new ObjectMapper().writeValueAsString(contentMap);
		} catch (IOException e) {
			throw new InterfaceException(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode(), e);
		}

		requestConsole(ConfigLoader.getInstance().get(Config.CONSOLE_HOST),
					   ConfigLoader.getInstance().getInt(Config.CONSOLE_RPC_PORT),
					   "initIndexData",
					   content);
	}

	/**
	 * 수집된 로그를 전송할 노드 정보 조회
	 * @param logpolicyId 정책아이디
	 * @return 데이터를 전송할 노드정보
	 * @throws InterfaceException
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, String>> getPolicyNode(String logpolicyId) throws InterfaceException {
		Map<String, String> contentMap = new HashMap<String, String>();
		contentMap.put("version", "1.0.0");
		contentMap.put("logpolicyId", logpolicyId);

		String content = null;
		try {
			content = new ObjectMapper().writeValueAsString(contentMap);
		} catch (IOException e) {
			throw new InterfaceException(CollectorCode.Code.INVALID_REQUEST_MESSAGE.getCode(), e);
		}

		String result = requestConsole(ConfigLoader.getInstance().get(Config.CONSOLE_HOST),
									   ConfigLoader.getInstance().getInt(Config.CONSOLE_RPC_PORT),
									   "getPolicyNode",
									   content);

		if(Strings.isNullOrEmpty(result)) throw new InterfaceException(CollectorCode.Code.INVALID_RESPONSE_MESSAGE.getCode());

		try {
			Map<String, Object> responseMap = new ObjectMapper().readValue(result, new TypeReference<Map<String, Object>>() {});
			return (List<Map<String, String>>)responseMap.get("result");
		} catch (IOException e) {
			throw new InterfaceException(CollectorCode.Code.INVALID_RESPONSE_MESSAGE.getCode());
		}
	}	

	/*********** Client Function END ***********/
	/*******************************************/
}