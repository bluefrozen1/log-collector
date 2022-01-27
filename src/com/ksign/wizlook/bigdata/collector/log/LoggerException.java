package com.ksign.wizlook.bigdata.collector.log;

public class LoggerException extends Exception {
	private static final long serialVersionUID = 4062087889731920198L;

	public LoggerException() {
		super();
	}

	public LoggerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public LoggerException(String message, Throwable cause) {
		super(message, cause);
	}

	public LoggerException(String message) {
		super(message);
	}

	public LoggerException(Throwable cause) {
		super(cause);
	}
}
