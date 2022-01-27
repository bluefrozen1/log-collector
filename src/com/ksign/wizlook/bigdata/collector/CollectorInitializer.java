package com.ksign.wizlook.bigdata.collector;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ksign.license.exception.LicenseException;
import com.ksign.license.verifier.NewLicenseVerifier;
import com.ksign.wizlook.bigdata.collector.collect.agent.receiver.AgentTcpReceiverManager;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectHistoryDashboardInsertThread;
import com.ksign.wizlook.bigdata.collector.collect.history.CollectHistoryInsertThread;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJobManager;
import com.ksign.wizlook.bigdata.collector.collect.purge.PurgeBackupDataScheduler;
import com.ksign.wizlook.bigdata.collector.collect.purge.PurgeCollectHistoryScheduler;
import com.ksign.wizlook.bigdata.collector.collect.push.PushReceiveLogRollingThread;
import com.ksign.wizlook.bigdata.collector.collect.push.PushReceiverManager;
import com.ksign.wizlook.bigdata.collector.collect.send.FileSendThread;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;
import com.ksign.wizlook.bigdata.collector.itf.avro.CollectorInterfaceService;
import com.ksign.wizlook.bigdata.collector.log.FileRollingLogger.RollingIntervalUnit;
import com.ksign.wizlook.bigdata.collector.log.Log4j2ForceRollingThread;
import com.ksign.wizlook.bigdata.collector.log.LoggerManager;
import com.ksign.wizlook.common.WizLookException;
import com.ksign.wizlook.common.crypto.KsignJCEUtils;

/**
 * Collector 초기화 및 구동 클래스
 * @author byw
 */
public class CollectorInitializer {
	/** collector -> engine 파일 전송 Thread */
	private Thread fileSendThread;
	/** 수집이력 Dashboard DB 저장 클래스 */
	private Thread collectHistoryDashboardInsertThread;
	/** 수집이력 저장 클래스 */
	private Thread collectHistoryInsertThread;
	/** log4j2 강제 Rolling Thread ( 추가 로그가 발생하지 않으면 Rolling되지 않기 때문 ) */
	private Thread log4j2ForceRollingThread;
	/** Push 수신 로그 Rolling Thread ( 추가 로그를 수신하지 않으면 Rolling되지 않기 때문 ) */
	private Thread pushReceiveLogRollingThread;
	/** logger */
	private Logger logger = LogManager.getLogger();

	/**
	 * 각 모듈 초기화
	 *  1. 설정파일 로딩
	 *  2. 라이센스 검증
	 *  3. 암호화 모듈 초기화
	 *  4. Poll Scheduler 초기화
	 *  5. 수집이력 Logger 초기화
	 * @param configFile 설정파일 경로
	 * @throws Exception
	 */
	public void init( String configFile ) throws Exception {
		// 1. 설정파일 로딩
		if( configFile == null || configFile.length() <= 0 ) {
			ConfigLoader.getInstance( null );
		} else {
			ConfigLoader.getInstance( new File(configFile) );
		}

		// 2. 라이센스 검증
		NewLicenseVerifier newLicenseVerifier = NewLicenseVerifier.getInstance(ConfigLoader.getInstance().get(Config.LICENSE_PATH));

		newLicenseVerifier.verifyLicenseSignature();
		if(!newLicenseVerifier.isVerifyLicenseSignature()) throw new LicenseException("is Verify License Signature");
		logger.info("### WizLook Collector License verify complete ###");

		// set Product Name
		ProductVersion.setProductVersion(newLicenseVerifier.getAttribute("Product Name"));
		logger.info("### WizLook Collector ( " + ProductVersion.getProductVertion() + " ) StartUp ###");

		// 3. 암호화 모듈 초기화
		KsignJCEUtils.init(ConfigLoader.getInstance().get(Config.CERT_CERTIFICATE),
						   ConfigLoader.getInstance().get(Config.CERT_PRIVATEKEY),
						   ConfigLoader.getInstance().get(Config.CERT_PRIVATEKEY_PWD));
		logger.info("	Initialize KsignCrypto");

		// 4. Poll Scheduler 초기화
		PollJobManager.INSTANCE.init();

		// 5. 수집이력 Logger 초기화
		LoggerManager.getInstance().addFileRollingLogger("collectHistoryLogger",
				 ConfigLoader.getInstance().get(Config.COLLECT_LOGGING_BASE_DIR) + File.separator + "collectHistory.log",
				 ConfigLoader.getInstance().get(Config.COLLECT_LOGGING_BASE_DIR) + File.separator + "collectHistory_%d{yyyyMMddHHmmss}.log",
				 RollingIntervalUnit.SECOND,
				 3L);
	}

	/**
	 * Collector 각 모듈 구동
	 */
	public void start() {
		try {
			// Start Agent Receiver (agent policy)
			AgentTcpReceiverManager.INSTANCE.start();
			logger.info("	Start AgentReceiver. port=[" + ConfigLoader.getInstance().get(Config.AGENT_RECEIVER_PORT) + "]");

			// Start Avro Interface server (console -> collector) 
			CollectorInterfaceService.INSTANCE.start();
			logger.info("	Start CollectorInterface");

			// Start 수집 이력 ( Dashboard 용 ) 저장 Thread
			if(ConfigLoader.getInstance().getBoolean(Config.LOG_DB_ENABLED)) {
				collectHistoryDashboardInsertThread = new Thread(new CollectHistoryDashboardInsertThread());
				collectHistoryDashboardInsertThread.start();
			}

			// Start 수집 이력 저장 Thread
			if(ConfigLoader.getInstance().getBoolean(Config.COLLECT_LOGGING_ENABLED)) {
				collectHistoryInsertThread = new Thread(new CollectHistoryInsertThread());
				collectHistoryInsertThread.start();
			}

			// Start FileSender ( collector -> engine node )
			fileSendThread = new Thread(new FileSendThread()); 
			fileSendThread.start();

			// Start Purge Backup log
			PurgeBackupDataScheduler.INSTANCE.start();
			logger.info("	Start PurgeBackupDataScheduler");

			// Start Purge Collect history Log Data
			PurgeCollectHistoryScheduler.INSTANCE.start();
			logger.info("	Start PurgeDetailLogScheduler");

			// Start log4j2 강제 Rolling Thread
			log4j2ForceRollingThread = new Thread(new Log4j2ForceRollingThread(1000));
			log4j2ForceRollingThread.start();

			// Start Push Receive log 강제 Rolling Thread
			if(ConfigLoader.getInstance().getBoolean(Config.PUSH_RECEIVE_BUFFER_SAVE_ENABLED)) {
				pushReceiveLogRollingThread = new Thread(new PushReceiveLogRollingThread(1000));
				pushReceiveLogRollingThread.start();
			}

			// request DataSource
			CollectorInterfaceService.INSTANCE.requestDataSource();
			logger.info("	Request DataSource");

			logger.info("### WizLook Collector ( " + ProductVersion.getProductVertion() + " ) StartUp complete ###");
		} catch(WizLookException e) {
			logger.error(this.getClass().getSimpleName(), e);
			System.exit(1);
		} catch(Throwable t) {
			logger.error(this.getClass().getSimpleName(), t);
			System.exit(1);
		} finally {
			// 99. shutdown
			Runnable shutdownRunnable = new Runnable() {
				public void run() {
					destroy();
				}
			};
			Runtime.getRuntime().addShutdownHook( new Thread(shutdownRunnable) );
		}
	}

	/**
	 * Collector shutdown 시 각 모듈 종료
	 */
	public void destroy() {
		logger.info("### WizLook Collector ( " + ProductVersion.getProductVertion() + " ) Shutdown Start ###");

		// Stop PushReceiver (push policy)
		PushReceiverManager.INSTANCE.destroy();
		logger.info("	Stop PushReceiverManager");

		// Stop JobScheduler (poll policy)
		PollJobManager.INSTANCE.destroy();
		logger.info("	Stop PollJobManager");

		// Stop PurgeDataScheduler
		PurgeBackupDataScheduler.INSTANCE.destroy();
		logger.info("	Stop PurgeBackupDataScheduler");

		// Stop PurgeCollectHistoryScheduler
		PurgeCollectHistoryScheduler.INSTANCE.destroy();
		logger.info("	Stop PurgeDetailLogScheduler");

		// Stop Avro Interface server (console -> collector)
		CollectorInterfaceService.INSTANCE.destroy();
		logger.info("	Stop CollectorInterfaceService");

		// Stop Agent Receiver (agent policy)
		AgentTcpReceiverManager.INSTANCE.destroy();
		logger.info("	Stop AgentTcpReceiverManager");

		// Stop FileSendThread ( collector -> engine node )
		fileSendThread.interrupt();

		if(ConfigLoader.getInstance().getBoolean(Config.LOG_DB_ENABLED)) {
			// Stop collectDashboardHistoryThread
			collectHistoryDashboardInsertThread.interrupt();
		}

		if(ConfigLoader.getInstance().getBoolean(Config.COLLECT_LOGGING_ENABLED)) {
			// Stop collectHistoryThread
			collectHistoryInsertThread.interrupt();
		}

		// Stop log4j2RollingThread
		log4j2ForceRollingThread.interrupt();

		if(ConfigLoader.getInstance().getBoolean(Config.PUSH_RECEIVE_BUFFER_SAVE_ENABLED)) {
			pushReceiveLogRollingThread.interrupt();
		}

		logger.info("### WizLook Collector ( " + ProductVersion.getProductVertion() + " ) Shutdown complete ###");
	}

	public static void main( String ... args ) throws Exception {
		CollectorInitializer initializer = new CollectorInitializer();
		if( args == null || args.length < 1 ) {
			initializer.init(null);
		} else {
			initializer.init(args[0]);
		}
		initializer.start();
	}
}