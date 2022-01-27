/**
 * 
 */
package com.ksign.wizlook.bigdata.collector.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;

/**
 * Collector Config Loading 클래스
 * @author byw
 */
public class ConfigLoader implements Config {
	private static volatile ConfigLoader instance = null;
	private File configFile;
	private Properties config;
	private final String defaultFilename = "collector.properties";
	/** logger */
	private final Logger logger = LogManager.getLogger();

	private ConfigLoader() {}

	private ConfigLoader( File configFile ) {
		this.configFile = configFile;
		load();
	}

	public static ConfigLoader getInstance() {
		if( instance == null ) throw new RuntimeException( "Cannot instance please call 'getInstance(File configFile)'" );

		return instance;
	}
	public static ConfigLoader getInstance( File configFile ) {
		if( instance == null ) 
			instance = new ConfigLoader( configFile );

		return instance;
	}

	/**
	 * Load config
	 */
	private void load() {
		if( configFile == null ) {
			String configPath = System.getProperty( "conf.loc" );
			if( Strings.isNullOrEmpty(configPath)) {
				throw new RuntimeException( "Cannot find config path. Please check the system property 'conf.loc'" );
			}
	
			this.configFile = new File( configPath + File.separator + defaultFilename );
		}
		if( !configFile.exists() || !configFile.isFile() )
			throw new RuntimeException( "Cannot find config file. Please check the file '" + configFile.getAbsolutePath() + "'" );

		FileInputStream fis = null;
		try {
			fis = new FileInputStream( configFile );

			config = new Properties();
			config.load( fis );
		} catch( FileNotFoundException e ) {
			logger.error(this.getClass().getName(), e);
			throw new RuntimeException( "Cannot find config file. Please check the file '"+ configFile.getAbsolutePath() + File.separator + configFile.getName() +"'" );
		} catch( IOException e ) {
			logger.error(this.getClass().getName(), e);
			throw new RuntimeException( "Cannot find config file. Please check the file '"+ configFile.getAbsolutePath() + File.separator + configFile.getName() +"'" );
		} finally {
			try { fis.close(); } catch( IOException ioEx ) {}
		}
	}

	public String get( String key ) {
		return config.getProperty( key );
	}

	public int getInt( String key ) {
		return Integer.parseInt( config.getProperty(key) );
	}

	public long getLong( String key ) {
		return Long.parseLong( config.getProperty(key) );
	}

	public boolean getBoolean( String key ) {
		return new Boolean( config.getProperty(key) );
	}
}