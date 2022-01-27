package com.ksign.wizlook.bigdata.collector.collect;

import com.ksign.wizlook.bigdata.collector.code.CollectorCode;
import com.ksign.wizlook.common.WizLookException;

public class CollectException extends WizLookException {
	private static final long serialVersionUID = -3764466473888050649L;

	public CollectException( CollectorCode.Code code ) {
		super( code.getCode() );
	}

	public CollectException( CollectorCode.Code code, String msg ) {
		super( code.getCode(), msg );
	}

	public CollectException( CollectorCode.Code code, Throwable throwable ) {
		super( code.getCode(), throwable );
	}
}