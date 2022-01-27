package com.ksign.wizlook.bigdata.collector.collect.poll;

import java.io.File;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.UnableToInterruptJobException;

import ch.ethz.ssh2.SFTPv3FileHandle;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectStatus;
import com.ksign.wizlook.bigdata.collector.collect.AbstractCollect;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectLogger;
import com.ksign.wizlook.bigdata.collector.itf.avro.CollectorInterfaceService;
import com.ksign.wizlook.bigdata.collector.itf.avro.InterfaceException;

/**
 * 모든 Polling Job들이 상속받아 구현하여야 하는 클래스
 * @author byw
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public abstract class PollJob extends AbstractCollect implements InterruptableJob {
	/** 수집 중지 여부 */
	private volatile boolean isInterruptedJob = false;
	/** 수집 로그 인코딩 */
	protected String collectLogEncoding;
	/** 기존에 인덱싱 되어있던 data 초기화 여부 */
	private String initIndexDataYn;
	/** 수집하기 위한 MetaData */
	protected Map<String, String> jobDataMap;
	/** 현재 Job 동작 시간 */
	protected Date fireTime;
	/** 인덱싱 데이터 초기화 요청을 한번만 하기 위한 플래그 */
	private boolean isInitIndexData = false;
	/** 수집 이력 로거 ( 수집 시작, 상세, 종료 ) */
	protected CollectLogger collectLogger;
	/** 수집 파일 크기 */
	private long collectFileSize;

	/**
	 * Quartz 에 의해 동작되는 Job
	 * @param jobExecutionContext
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		long startTime = System.currentTimeMillis();
		boolean result = false;
		CollectStatus pollJobStatus = CollectStatus.RUNNING;
		try {
			// JOB 구동에 필요한 데이터(JOB SCHEDULE 최초 등록 시 입력된 데이터)
			this.jobDataMap = (HashMap<String, String>) jobExecutionContext.getJobDetail().getJobDataMap().get("jobDataMap");

			this.logpolicyId = (String) jobExecutionContext.getJobDetail().getJobDataMap().get("logpolicyId");
			this.dataSourceId = (String) jobExecutionContext.getJobDetail().getJobDataMap().get("dataSourceId");
			this.collectLogEncoding = (String) jobExecutionContext.getJobDetail().getJobDataMap().get("collectLogEncoding");
			this.initIndexDataYn = (String) jobExecutionContext.getJobDetail().getJobDataMap().get("initIndexDataYn");
			this.fireTime = jobExecutionContext.getScheduledFireTime();
			this.collectLogger = new CollectLogger(logpolicyId, dataSourceId);

			collectLogger.loggingCollectStart(startTime);
			collectLogger.loggingCollectDetailLog("[Scheduler] START Job");

			result = start();

		} catch (Throwable t) {
			collectLogger.loggingCollectDetailLog("[Exception] ", t);
			logger.error(this.getClass().getSimpleName(), t);
		} finally {
			if(isInterruptedJob) {
				collectLogger.loggingCollectDetailLog("[Scheduler] KILLED Job");
				pollJobStatus = CollectStatus.KILLED;
			} else {
				collectLogger.loggingCollectDetailLog("[Scheduler] END Job");
				if(result) pollJobStatus = CollectStatus.SUCCESS;
				else pollJobStatus = CollectStatus.ERROR;
			}
			long endTime = System.currentTimeMillis();
			collectLogger.loggingCollectEnd(endTime, collectFileSize, pollJobStatus);
		}
	}

	/**
	 * Job 동작 중지
	 */
	@Override
	public void interrupt() throws UnableToInterruptJobException {
		this.isInterruptedJob = true;
	}

	/**
	 * Job의 interrupt여부 반환
	 * @return
	 */
	protected boolean isInterruptedJob() {
		return this.isInterruptedJob;
	}

	/**
	 * 해당 정책의 인덱싱 데이터 초기화 요청을 한다.
	 * 요청 흐름 ( collector -> console -> engine )
	 * @param logpolicyId 정책아이디
	 * @throws CollectException 인덱싱 데이터 초기화 요청 실패 시 발생
	 */
	private void initIndexData(String logpolicyId) throws CollectException {
		if("Y".equals(initIndexDataYn) && !isInitIndexData) {
			collectLogger.loggingCollectDetailLog("[Initialize] Delete all indexing data");
			try {
				CollectorInterfaceService.INSTANCE.initIndexData(logpolicyId);
			} catch(InterfaceException e) {
				logger.error(this.getClass().getSimpleName(), e);
				throw new CollectException(CollectorCode.Code.FAIL_INIT_INDEX_DATA);
			}
			isInitIndexData = true;
		}
	}

	@Override
	public long save(byte[] byteData, String fileName, String logEncoding, boolean append) throws CollectException {
		initIndexData(logpolicyId);
		long fileSize = super.save(byteData, fileName, logEncoding, append);
		collectFileSize += fileSize;
		return fileSize;
	}

	@Override
	public long save(InputStream inputStream, String fileName, String logEncoding, boolean append) throws CollectException {
		initIndexData(logpolicyId);
		long fileSize = super.save(inputStream, fileName, logEncoding, append);
		collectFileSize += fileSize;
		return fileSize;
	}

	@Override
	public long save(SFTPv3FileHandle sftpHandle, String fileName, String logEncoding, boolean append) throws CollectException {
		initIndexData(logpolicyId);
		long fileSize = super.save(sftpHandle, fileName, logEncoding, append);
		collectFileSize += fileSize;
		return fileSize;
	}

	@Override
	public long save(File file, String fileName, String logEncoding, boolean append) throws CollectException {
		initIndexData(logpolicyId);
		long fileSize = super.save(file, fileName, logEncoding, append);
		collectFileSize += fileSize;
		return fileSize;
	}

	/**
	 * Job 수행
	 * @param jobDataMap Job수행 시 필요한 DataMap
	 * @throws Exception 
	 */
	public boolean start() throws Exception {
		String paramName = null;
		if((paramName = this.validateParameter(jobDataMap)) != null) {
			collectLogger.loggingCollectDetailLog("[Check parameter] Invalid parameter.. paramName=[" + paramName + "]");
			return false;
		}

		return this.collect(jobDataMap);
	}
	
	/**
	 * 각 Job별로 필수 데이터 유효성 체크
	 * @param jobDataMap Job 수행 시 필요한 데이터
	 * @return String 유효성 체크 결과 (null 일 경우 정상, String값일 경우 invalid Parameter name)
	 */
	protected abstract String validateParameter(Map<String, String> jobDataMap);
	
	
	/**
	 * 각 Protocol 별 데이터 수집 
	 * @param jobDataMap Job 수행 시 필요한 데이터
	 * @throws Exception
	 */
	protected abstract boolean collect(Map<String, String> jobDataMap) throws Exception;

	/**
	 * stop collect method.. not use..
	 */
	@Override
	public boolean stop() {
		return false;
	}
}