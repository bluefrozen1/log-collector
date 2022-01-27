package com.ksign.wizlook.bigdata.collector.util;

import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;


public class InfluxUtil {
	private static volatile InfluxUtil instance;
	public static synchronized InfluxUtil getInstance() {
		if( instance == null )
			instance = new InfluxUtil();

		return instance;
	}

	private InfluxUtil() {
		ConfigLoader.getInstance().get("influx.url");
		ConfigLoader.getInstance().get("influx.id");
		ConfigLoader.getInstance().get("influx.pw");
		
	}
}
