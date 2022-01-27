package com.ksign.wizlook.bigdata.collector.collect.poll;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.utils.Key.DEFAULT_GROUP;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import com.google.common.base.Strings;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.CollectAction;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.ExecuteType;
import com.ksign.wizlook.bigdata.collector.code.CollectorCode.IntervalUnit;
import com.ksign.wizlook.bigdata.collector.collect.CollectException;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.util.XmlParser;

/**
 * Polling 수집 동작을 관리하는 Manager 클래스
 * @author byw
 */
public enum PollJobManager {
	INSTANCE;
	/** job scheduler  */
	private Scheduler scheduler = null;
	/** default quartz thread count */
	private String defaultSchedulerThreadCount = "10";
	/** logger */
	private final Logger logger = LogManager.getLogger();

	public synchronized void init() throws SchedulerException {
		if(scheduler == null) {
			// Quartz 가 사용할 Thread Count
			String schedulerThreadCount = ConfigLoader.getInstance().get(Config.POLL_SCHEDULER_THREAD_COUNT);
			if(!Strings.isNullOrEmpty(schedulerThreadCount)) {
				defaultSchedulerThreadCount = schedulerThreadCount; 
			}

			Properties prop = new Properties();
			prop.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
			prop.put("org.quartz.threadPool.threadCount", defaultSchedulerThreadCount);
			prop.put("org.quartz.threadPool.threadPriority", "5");

			scheduler = new StdSchedulerFactory(prop).getScheduler();
			if (!scheduler.isStarted()) {
				scheduler.start();
			}
		}
	}

	/**
	 * 크론표현식에 따른 Job 실행
	 * @param jobDetail Job 상세
	 * @param jobName Job 이름
	 * @param cronScript 크론포현식
	 * @throws SchedulerException
	 */
	private void startCronJob(JobDetail jobDetail, String jobName, String cronScript, Date startDate) throws SchedulerException {

		Trigger trigger = null;
		if(startDate != null && startDate.compareTo(new Date()) > 0) {
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(jobName + "Trigger", DEFAULT_GROUP)
					.withSchedule(CronScheduleBuilder.cronSchedule(cronScript))
					.startAt(startDate)	// startDate부터 시작(startDate가 과거일 경우 그 시간으로 부터 interval을 계산하여 실행됨)
					.build();
		} else {
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(jobName + "Trigger", DEFAULT_GROUP)
					.withSchedule(CronScheduleBuilder.cronSchedule(cronScript))
					.startNow()	// 즉시 시작
					.build();
		}
		scheduler.scheduleJob(jobDetail, trigger);
	}

	/**
	 * 단순 반복 작업하는 Job Schedule 실행
	 * @param jobDetail Job 상세
	 * @param jobName Job 이름
	 * @param intervalUnit 실행주기 단위
	 * @param interval 실행 주기
	 * @param startDate 최초 실행 날짜
	 * @throws SchedulerException
	 * @throws ParseException 
	 */
	private void startRepeatJob(JobDetail jobDetail, String jobName, String intervalUnit, int interval, Date startDate) throws SchedulerException {

		SimpleScheduleBuilder builder = SimpleScheduleBuilder.simpleSchedule();

		if(IntervalUnit.MILLISECOND.toString().equals(intervalUnit)) {
			builder = builder.withIntervalInMilliseconds(interval);
		} else if(IntervalUnit.SECOND.toString().equals(intervalUnit)) {
			builder = builder.withIntervalInSeconds(interval);
		} else if(IntervalUnit.MINUTE.toString().equals(intervalUnit)) {
			builder = builder.withIntervalInMinutes(interval);
		} else if(IntervalUnit.HOUR.toString().equals(intervalUnit)) {
			builder = builder.withIntervalInHours(interval);
		} else {
			throw new SchedulerException("Known Schedule Interval Unit Type!");
		}

		Trigger trigger = null;
		if(startDate == null) {
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(jobName + "Trigger", DEFAULT_GROUP)
					.withSchedule(builder.repeatForever())
					.startNow()	// 즉시 시작
					.build();
		} else {
			trigger = TriggerBuilder.newTrigger()
					.withIdentity(jobName + "Trigger", DEFAULT_GROUP)
					.withSchedule(builder.repeatForever())
					.startAt(startDate)	// startDate부터 시작(startDate가 과거일 경우 그 시간으로 부터 interval을 계산하여 실행됨)
					.build();
		}
		scheduler.scheduleJob(jobDetail, trigger);
	}

	/**
	 * 한번 작업하는 Job Schedule 실행
	 * @param jobDetail Job 상세
	 * @param jobName Job 이름
	 * @throws SchedulerException
	 * @throws ParseException 
	 */
	private void startOnetimeJob(JobDetail jobDetail, String jobName) throws SchedulerException {

		Trigger trigger = null;
		trigger = TriggerBuilder.newTrigger()
								.withIdentity(jobName + "Trigger", DEFAULT_GROUP)
								.withSchedule(SimpleScheduleBuilder.repeatSecondlyForTotalCount(1))
								.startNow()	// 즉시 시작
								.build();
		scheduler.scheduleJob(jobDetail, trigger);
	}

	public boolean manageJob(List<Map<String, String>> dataSourceMapList) throws CollectException {
		try {
			for(Map<String, String> dataSourceMap : dataSourceMapList) {
				if(CollectorCode.CollectAction.RUNNING.toString().equals(dataSourceMap.get("jobAction"))) {
					startJob(dataSourceMap);
				} else if(CollectorCode.CollectAction.STOP.toString().equals(dataSourceMap.get("jobAction"))) {
					stopJob(dataSourceMap.get("dataSourceId"));
				}
			}
		} catch(CollectException e) {
			for(Map<String, String> dataSourceMap : dataSourceMapList) {
				try {
					stopJob(dataSourceMap.get("dataSourceId"));
				} catch(CollectException e1) {
					logger.error(this.getClass().getSimpleName(), e);
				}
			}
			throw e;
		}
		return true;
	}
	/**
	 * Job 을 기동
	 * @param dataSourceMap Job을 기동하기 위한 정보를 담은 Map
	 * @return 기동 결과 반환
	 * @throws CollectException 
	 */
	public boolean startJob(Map<String, String> dataSourceMap) throws CollectException {
		
		try {
			// scheduler에 등록된 동작중인 job인지 확인
			JobDetail jobDetailOld = getJobDetail(dataSourceMap.get("dataSourceId"));
	
			// scheduler에 등록되진 않았지만 아직 동작중인 job인지 확인
			// job을 stop하였지만 아직 job이 완전히 끝나지 않은 케이스가 이에 해당
			if(jobDetailOld == null) {
				jobDetailOld = getCurrentExecutionJobDetail(dataSourceMap.get("dataSourceId"));
			}
	
			// Job이 존재할 경우 제거 Error
			if (jobDetailOld != null) {
				throw new CollectException(CollectorCode.Code.ALREADY_RUNNING_JOB);
			}
	
			@SuppressWarnings("unchecked")
			JobDetail jobDetail = newJob((Class<? extends Job>) Class.forName(dataSourceMap.get("implClassPath")))
										.withIdentity(dataSourceMap.get("dataSourceId"), DEFAULT_GROUP)
										.build();
	
			Map<String, String> jobDataMap = new XmlParser().getXmlDataToMap(dataSourceMap.get("jobConfigData"));
			jobDataMap.put("dataServerHost", dataSourceMap.get("dataServerHost"));
			jobDetail.getJobDataMap().put("jobDataMap", jobDataMap);
			jobDetail.getJobDataMap().put("logpolicyId", dataSourceMap.get("logpolicyId"));
			jobDetail.getJobDataMap().put("dataSourceId", dataSourceMap.get("dataSourceId"));
			jobDetail.getJobDataMap().put("collectLogEncoding", dataSourceMap.get("collectLogEncoding"));
			jobDetail.getJobDataMap().put("initIndexDataYn", dataSourceMap.get("initIndexDataYn"));

			Date startDate = null;
			if(!Strings.isNullOrEmpty(dataSourceMap.get("jobStartDatetime"))) {
				SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
				startDate = df.parse(dataSourceMap.get("jobStartDatetime"));
			}

			if(ExecuteType.CRON_EXPRESSION.toString().equals(dataSourceMap.get("jobExecuteType"))) {
	
				if(!CronExpression.isValidExpression(dataSourceMap.get("jobCronExpression"))) {
					throw new CollectException(CollectorCode.Code.INVALID_CRON_EXPRESSION);
				}

				startCronJob(jobDetail, dataSourceMap.get("dataSourceId"), dataSourceMap.get("jobCronExpression"), startDate);
			} else if(ExecuteType.INTERVAL.toString().equals(dataSourceMap.get("jobExecuteType"))) {
				startRepeatJob(jobDetail, dataSourceMap.get("dataSourceId"), dataSourceMap.get("jobIntervalUnit"), Integer.parseInt(dataSourceMap.get("jobInterval")), startDate);
			} else if(ExecuteType.ONE_OFF.toString().equals(dataSourceMap.get("jobExecuteType"))) {
				startOnetimeJob(jobDetail, dataSourceMap.get("dataSourceId"));
			} else {
				throw new CollectException(CollectorCode.Code.INVALID_EXECUTE_TYPE);
			}
		} catch(ParseException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.INVALID_JOB_START_DATETIME);
		} catch(SchedulerException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_START_JOB);
		} catch(CollectException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw e;
		} catch(Exception e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.UNKNOWN_ERROR);
		}

		return true;
	}

	/**
	 * 해당 잡 스케줄러 종료(제거) 후 DB갱신
	 * @param dataSourceId
	 * @return Job 정지 결과
	 * @throws CollectException 
	 * @throws SchedulerException 
	 */
	public boolean stopJob(String dataSourceId) throws CollectException {
		boolean result = false;
		try {
			interruptJob(dataSourceId); 
			JobDetail jobDetail = getJobDetail(dataSourceId);
	
			if(jobDetail == null) {
				result = true;
			} else {
				if (scheduler.checkExists(jobDetail.getKey())) {
					if(getCurrentExecutionJobDetail(dataSourceId) != null) {
						scheduler.interrupt(jobDetail.getKey());
					}
					if (scheduler.deleteJob(jobDetail.getKey())) {
						result = true;
					}
				}
			}
		} catch(SchedulerException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_STOP_JOB, e);
		}
		return result;
	}

	/**
	 * 현재 동작중인 job을 강제로 중지시킨다.
	 * @param dataSourceId
	 * @return
	 * @throws SchedulerException
	 */
	public boolean interruptJob(String dataSourceId) throws SchedulerException {
		JobDetail jobDetail = getCurrentExecutionJobDetail(dataSourceId);

		if (jobDetail == null) return true;
		return scheduler.interrupt(jobDetail.getKey());
	}

	/**
	 * 잡 스케줄러 상태 반환 (상태코드 정의 필요)
	 * @param dataSourceId 스케줄 아이디
	 * @return Job 상태
	 * @throws CollectException 
	 * @throws SchedulerException 
	 */
	public Map<String, Map<String, String>> getJobStatus(String dataSourceId) throws CollectException {
		Map<String, Map<String, String>> resultMap = new HashMap<String, Map<String, String>>();
		try {

			// Collector에 동작 중인 전체 dataSource의 상태 조회 (dataSourceId가 null 인 경우)
			if(Strings.isNullOrEmpty(dataSourceId)) {
				for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(DEFAULT_GROUP))) {
					Map<String, String> jobInfo = getJobDetailStatus(scheduler.getJobDetail(jobKey));
					resultMap.put(jobKey.getName(), jobInfo);
				}
	
			// Job 상태조회 (dataSourceId가 null이 아닌 경우)
			} else {
				JobDetail jobDetail = getJobDetail(dataSourceId);
				if(jobDetail == null) jobDetail = getCurrentExecutionJobDetail(dataSourceId);
				if(jobDetail != null) {
					Map<String, String> jobInfo = getJobDetailStatus(jobDetail);
					resultMap.put(dataSourceId, jobInfo);
				}
			}
		} catch(SchedulerException e) {
			logger.error(this.getClass().getSimpleName(), e);
			throw new CollectException(CollectorCode.Code.FAIL_GET_JOB_STATUS);
		}

		return resultMap;
	}

	/**
	 * Job 상세 정보 조회
	 * @param jobDetail Quartz JobDetail
	 * @return 상세 정보 반환
	 * @throws SchedulerException 
	 */
	private Map<String, String> getJobDetailStatus(JobDetail jobDetail) throws SchedulerException {
		Map<String, String> jobInfo = new HashMap<String, String>();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		String status = CollectAction.STOP.toString();
		String firstExeTimeStr = "";
		String preExeTimeStr = "";
		String nextExeTimeStr = "";

		// Job 상태조회
		if (jobDetail != null && scheduler.checkExists(jobDetail.getKey())) {
			TriggerKey triggerKey = scheduler.getTriggersOfJob(jobDetail.getKey()).get(0).getKey();
			TriggerState triggerState = scheduler.getTriggerState(triggerKey);
			
			if(TriggerState.NORMAL.equals(triggerState) || TriggerState.BLOCKED.equals(triggerState)) {
				status = CollectAction.RUNNING.toString();
			} else if(TriggerState.COMPLETE.equals(triggerState) || TriggerState.PAUSED.equals(triggerState) || TriggerState.NONE.equals(triggerState)) {
				status = CollectAction.STOP.toString();
			} else {
				status = CollectAction.STOP.toString();
			}
			Date firstExeTime = scheduler.getTriggersOfJob(jobDetail.getKey()).get(0).getStartTime();
			Date previousExeTime = scheduler.getTriggersOfJob(jobDetail.getKey()).get(0).getPreviousFireTime();
			Date nextExeTime = scheduler.getTriggersOfJob(jobDetail.getKey()).get(0).getNextFireTime();

			if(firstExeTime != null) firstExeTimeStr = df.format(firstExeTime);
			if(previousExeTime != null) preExeTimeStr = df.format(previousExeTime);
			if(nextExeTime != null) nextExeTimeStr = df.format(nextExeTime);
		}

		jobInfo.put("status", status);
		jobInfo.put("firstExeTime", firstExeTimeStr);
		jobInfo.put("preExeTime", preExeTimeStr);
		jobInfo.put("nextExeTime", nextExeTimeStr);
		return jobInfo;
	}

	/**
	 * jobId를 통해 현재 스케줄러에 등록된 job을 조회
	 * @param jobId
	 * @return
	 * @throws SchedulerException 
	 */
	private JobDetail getJobDetail(String jobId) throws SchedulerException {
		if(Strings.isNullOrEmpty(jobId)) return null;
		for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(DEFAULT_GROUP))) {
			if(jobKey.getName().equals(jobId)) return scheduler.getJobDetail(jobKey);
		}
		return null;
	}

	/**
	 * jobId를 통해 현재 동작중인 job을 조회
	 * @param jobId
	 * @return
	 */
	private JobDetail getCurrentExecutionJobDetail(String jobId) {
		if(Strings.isNullOrEmpty(jobId)) return null;
		try {
			for(JobExecutionContext jobContext : scheduler.getCurrentlyExecutingJobs()) {
				if(jobId.equals(jobContext.getJobDetail().getKey().getName())) return jobContext.getJobDetail(); 
			}
		} catch (SchedulerException e) {
			logger.error(this.getClass().getSimpleName(), e);
		}
		return null;
	}

	/**
	 * 잡 스케줄러 종료 (Spring destroy시 호출)
	 */
	public void destroy() {
		try {
			if (scheduler != null && !scheduler.isShutdown()) {
				for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(DEFAULT_GROUP))) {
					try { scheduler.interrupt(jobKey); } catch(UnableToInterruptJobException e) { logger.error(this.getClass().getSimpleName(), e);}
				}
				// shutdown 대기
				scheduler.shutdown(true);
			}
			scheduler = null;
		} catch (SchedulerException e) {
			logger.error(this.getClass().getName(), e);
		}
	}
}