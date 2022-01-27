package com.ksign.wizlook.bigdata.collector.code;

/**
 * Collector에서 사용하는 Code 클래스
 * @author byw
 */
public class CollectorCode {

	/** Collector Code */
	public static enum Code {
		// common
		SUCCESS("CLT0000000"),
		UNKNOWN_ERROR("CLT0000001"),

		// interface
		FAIL_INIT_INDEX_DATA("CLT0100001"),
		FAIL_REGIST_LOGPOLICY_PATTERN("CLT0100002"),

		NOT_FOUND_REQUEST_MESSAGE("CLT0100101"),
		INVALID_REQUEST_MESSAGE("CLT0100102"),
		NOT_FOUND_FUNCTION_NAME("CLT0100103"),
		INVALID_FUNCTION_NAME("CLT0100104"),
		NOT_FOUND_RESPONSE_MESSAGE("CLT0100105"),
		INVALID_RESPONSE_MESSAGE("CLT0100106"),
		FAIL_REQUEST("CLT0100107"),

		// collect common
		FAIL_SAVE_LOG("CLT0200001"),
		FAIL_SEND_LOG_TO_NODE("CLT0200002"),

		// collect poll
		FAIL_GET_JOB_STATUS("CLT0200101"),
		FAIL_START_JOB("CLT0200102"),
		FAIL_STOP_JOB("CLT0200103"),
		ALREADY_RUNNING_JOB("CLT0200104"),
		INVALID_CRON_EXPRESSION("CLT0200111"),
		INVALID_EXECUTE_TYPE("CLT0200112"),
		INVALID_JOB_ACTION("CLT0200113"),
		INVALID_JOB_START_DATETIME("CLT0200114"),

		// collect push
		FAIL_GET_RECEIVER_STATUS("CLT0200201"),
		FAIL_START_RECEIVER("CLT0200202"),
		FAIL_STOP_RECEIVER("CLT0200203"),
		NOT_AVAILABLE_PORT("CLT0200211"),
		INVALID_RECEIVER_ACTION("CLT0200212"),
		NOT_FOUND_IMPL_CLASSPATH("CLT0200213"),
		INVALID_IMPL_CLASSPATH("CLT0200214"),
		
		// collect history
		FAIL_LOAD_HISTORY("CLT0200301"),
		NOT_FOUND_HISTORY("CLT0200302");

		private final String code;
        private Code(String code) { this.code = code; }
		public String getCode() { return code; }
	}

	/** 결과 메시지 */
	public static enum RtMessage {
		SUCCESS,
		FAIL;
	}

	/** Collector 기동 상태 */
	public static enum Status {
		INIT,
		START,
		STOP,
		DESTROY;
	}

	/** 스케줄러 수행주기 단위 */
	public static enum IntervalUnit {
		MILLISECOND,
		SECOND,
		MINUTE,
		HOUR,
		DAY,
		MONTH,
		YEAR;
	}

	/** 잡 스케줄러 수행모드 */
	public static enum ExecuteType {
		CRON_EXPRESSION,
		INTERVAL,
		ONE_OFF;
	}

	/** PushReceiver / PollScheduler 동작 상태 */
	public static enum CollectAction {
		RUNNING,
		STOP,
		KILL
	}

	/** 수집 상태 */
	public static enum CollectStatus {
		SUCCESS,
		RUNNING,
		KILLED,
		ERROR
	}

	/** FTP 접속 유형 */
	public static enum FtpConnectMode {
		ACTIVE,
		PASSIVE
	}
	
	/** 통신 프로토콜 */
	public static enum CommProtocol {
		TCP,
		UDP
	}

	/** 사용여부 Y / N */
	public static enum UseYn {
		Y,
		N
	}

	/** 로그 보관주기 단위 */
	public static enum RetentionUnit {
		DAY,
		MONTH
	}
}