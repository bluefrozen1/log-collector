package com.ksign.wizlook.bigdata.collector.collect.agent;

import com.ksign.wizlook.bigdata.collector.collect.AbstractCollect;

/**
 * Agent로부터 수신받은 로그를 저장하기 위해 사용되는 클래스
 *  - Agent는 다른 수집유형과 다르게 Receiver가 고정적으로 listen하고 있다.
 *  - 다른 수집유형과 같은 수집데이터 저장 로직을 사용하기 위한 구색을 맞추기 위해 AgentCollect 클래스를 사용하여 저장한다.
 * @author byw
 */
public class AgentCollect extends AbstractCollect {

	@Override
	public boolean start() throws Exception {
		throw new UnsupportedOperationException("Not use method.");
	}

	@Override
	public boolean stop() throws Exception {
		throw new UnsupportedOperationException("Not use method.");
	}

	public AgentCollect(String logpolicyId, String dataSourceId) {
		super.init(logpolicyId, dataSourceId);
	}
}