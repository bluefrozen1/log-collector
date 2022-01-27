package com.ksign.wizlook.bigdata.collector.collect.agent.receiver;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * AgentTcpReceiver 관리 클래스
 * @author byw
 */
public enum AgentTcpReceiverManager {
	INSTANCE;
	private AgentTcpReceiver agentTcpReceiver;

	private AgentTcpReceiverManager() { }

	/**
	 * Agent Receiver 구동
	 * 구동할 Agent Receiver class의 경로는 properties에 설정되어 있다.
	 * @return 구동 결과 반환
	 * @throws Exception
	 */
	public boolean start() throws Exception {

		if(!ConfigLoader.getInstance().getBoolean(Config.AGENT_RECEIVER_ENABLED)) return false;
		if(agentTcpReceiver != null) return true;

		String implClassPath = ConfigLoader.getInstance().get(Config.AGENT_RECEIVER_IMPL_CLASS_PATH);
		int port = ConfigLoader.getInstance().getInt(Config.AGENT_RECEIVER_PORT);

		agentTcpReceiver = AgentReceiverFactory.getNewInstance(implClassPath);
		return agentTcpReceiver.start(port);
	}

	/**
	 * Agent Receiver 종료
	 * @return 종료 결과 반환
	 */
	public boolean destroy() {

		if(agentTcpReceiver == null) return true;
		try { return agentTcpReceiver.stop(); } catch (Exception e) {} 
		return false;
	}

	/**
	 * 구현된 Agent Receiver 클래스 로딩
	 */
	public static class AgentReceiverFactory {
		public static AgentTcpReceiver getNewInstance(String classPath) throws Exception {
			if(Strings.isNullOrEmpty(classPath)) throw new Exception("Not found pushClassPath.."); 
			try {
				Class<?> cls = Class.forName(classPath);
				return (AgentTcpReceiver) cls.newInstance();
			} catch (ClassNotFoundException e) {
				throw e;
			} catch (InstantiationException e) {
				throw e;
			} catch (IllegalAccessException e) {
				throw e;
			}
		}
	}
}