package com.ksign.wizlook.bigdata.collector.itf.avro;

import com.ksign.wizlook.common.WizLookException;

public class InterfaceException extends WizLookException {
	private static final long serialVersionUID = -3764466473888050649L;

	public InterfaceException(String code) {
		super(code);
	}

	public InterfaceException(String code, Throwable throwable) {
		super(code, throwable);
	}
}