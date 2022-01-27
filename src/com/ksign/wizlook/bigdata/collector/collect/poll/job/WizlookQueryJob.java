package com.ksign.wizlook.bigdata.collector.collect.poll.job;

import java.io.File;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.google.common.base.Strings;
import com.ksign.wizlook.api.AdhocSearchClient;
import com.ksign.wizlook.api.bean.AdhocSearchResult;
import com.ksign.wizlook.bigdata.collector.collect.poll.PollJob;
import com.ksign.wizlook.bigdata.collector.config.Config;
import com.ksign.wizlook.bigdata.collector.config.ConfigLoader;

/**
 * 수집유형 WIZLOOK_QUERY 구현 클래스
 *  - WizLook 에 adhoc query를 요청하여 query결과를 수집한다.
 * @author byw
 */
public class WizlookQueryJob extends PollJob {

	private static final String SEPARATOR = "Ω";

	@Override
	protected String validateParameter(Map<String, String> jobDataMap) {

		// Wizlook console host
		if(Strings.isNullOrEmpty(jobDataMap.get("dataServerHost"))) return "dataServerHost";
		// Wizlook console port
		if(Strings.isNullOrEmpty(jobDataMap.get("port"))) return "port";
		// Wizlook Query
		if(Strings.isNullOrEmpty(jobDataMap.get("wizlookQuery"))) return "query";

		return null;
	}

	@Override
	protected boolean collect(Map<String, String> jobDataMap) throws Exception {
		String host = jobDataMap.get("dataServerHost");
		String port = jobDataMap.get("port");
		String wizlookQuery = jobDataMap.get("wizlookQuery");

		// 1. wizlook Query를 통해 data조회
		// 2. CSV형태의 data의 separator를 Ω 로 수정하여 다시 저장
		//    ( JDBC 형태의 데이터처럼 구성하여 engine으로 넘기기 위한 처리. engine에서 JDBC search analyzer 사용 )
		// 3. engine에 전달하여 새로운 정책으로 다시 indexing

		File adhocResultFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator +
				 "WizlookQuery," + dataSourceId + "," + UUID.randomUUID().toString());

		File tempSaveFile = new File(ConfigLoader.getInstance().get(Config.COLLECT_DIR) + File.separator +
				 "WizlookQuery," + dataSourceId + "," + UUID.randomUUID().toString());

		collectLogger.loggingCollectDetailLog("[Adhoc query] host=[" + host + "], port=[" + port + "], query=[" + wizlookQuery + "]");

		// wizlook query 를 통해 data를 수신하여 파일에 저장 ( CSV )
		long startTime = System.currentTimeMillis();
		AdhocSearchResult adhocSearchResult = AdhocSearchClient.getQueryResultFileSync(host, port, "1.0.0", wizlookQuery, adhocResultFile);
		long elapsedTime = System.currentTimeMillis() - startTime;
		collectLogger.loggingCollectDetailLog("[Adhoc query] SUCCESS. elapsedTime=[" + elapsedTime + "(ms)]");

		FileWriter writer = null;
		CSVParser parser = null;
		try{
			// CSV형태의 data의 separator를 Ω 로 수정하여 다시 저장
			// JDBC 형태의 데이터처럼 구성하여 engine으로 넘기기 위한 처리
			collectLogger.loggingCollectDetailLog("[Convert data] change separator. before=[,] after=[" + SEPARATOR + "]");

			writer = new FileWriter(tempSaveFile);

			parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse( new InputStreamReader(new FileInputStream(adhocSearchResult.getResultFile())));

			for( Iterator<CSVRecord> it = parser.iterator(); it.hasNext(); ) {
				CSVRecord record = it.next();
				Iterator<String> iterValue = record.iterator();
				StringBuilder sb = new StringBuilder();
				while(iterValue.hasNext()) {
					sb.append(iterValue.next()).append(SEPARATOR);
				}
				sb.deleteCharAt(sb.length() - 1);
				sb.append(System.lineSeparator());
				writer.write(sb.toString());
			}

			if(writer != null) {
				writer.flush();
				writer.close();
			}
			collectLogger.loggingCollectDetailLog("[Convert data] SUCCESS");

			if(isInterruptedJob()) return false;
			if(tempSaveFile.length() > 0) {
				long saveFileSize = super.save(tempSaveFile, "WIZLOOK_QUERY", "", true);
				collectLogger.loggingCollectDetailLog("[Save file] SUCCESS. fileSize=[" + saveFileSize + "]");
			}
			return true;

		} catch(Exception e){
			throw e;
		} finally{
			if(adhocResultFile != null && adhocResultFile.exists()) adhocResultFile.delete();
			if(writer != null) { writer.close(); };
			if(tempSaveFile != null && tempSaveFile.exists()) tempSaveFile.delete();
			if(parser != null) { parser.close(); };
		}
	}
}