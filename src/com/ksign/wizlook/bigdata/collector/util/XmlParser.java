package com.ksign.wizlook.bigdata.collector.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.base.Strings;

public class XmlParser extends DefaultHandler {
	private String currentNodeName;
	private HashMap<String, String> dataMap;
	private SAXParser parser;
	/** logger */
	private static final Logger logger = LogManager.getLogger();
	
	public XmlParser() {
		
		super();
		
		try {
			parser = SAXParserFactory.newInstance().newSAXParser();
		} catch (ParserConfigurationException e) {
			logger.error(this.getClass().getName(), e);
		} catch (SAXException e) {
			logger.error(this.getClass().getName(), e);
		}
	}
	
	public HashMap<String, String> getXmlDataToMap(String xmlData) {

		if(Strings.isNullOrEmpty(xmlData)) {
			return new HashMap<String, String>();
		}

		try {
			
			parser.parse(new ByteArrayInputStream(xmlData.toString().getBytes("UTF-8")), this);
			return dataMap;
			
		} catch (SAXException e) {
			logger.error(this.getClass().getName(), e);
		} catch (IOException e) {
			logger.error(this.getClass().getName(), e);
		}
		
		return null;
	}
	 
	public static String mapToXml(Map map) {
		
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"" + "?>");
			sb.append("<data>");
			
			if (map != null) {
				for (Iterator<String> it = map.keySet().iterator(); it.hasNext();) {
					String field = it.next();
	
					sb.append("<" + field + ">");
					sb.append(map.get(field).toString() + "");
					sb.append("</" + field + ">");
				}
			}
			sb.append("</data>");
			
			return sb.toString();
			
		} catch (Exception e) {
			logger.error(XmlParser.class.getName(), e);
		}
		
		return null;
	}
	
	
	@Override
	public void startDocument() throws SAXException {
		super.startDocument();
		dataMap = new HashMap<String, String>();
	}
	
	@Override
	public void endDocument() throws SAXException {
		super.endDocument();
	}

	@Override
	public void startElement (String uri, String localName, String qName, Attributes atts) throws SAXException{
		currentNodeName = qName;
	}
 
	@Override
	public void endElement (String uri, String localName, String qName)throws SAXException {
	}

	
	@Override
	public void characters (char[] chars, int start, int length) throws SAXException{
		//xml에 줄바꿈이 존재할 경우 caracters 메소드가 여러번 호출된다. currentNodeName이 같을경우 append로 붙여서 처리
		if(dataMap.containsKey(currentNodeName)) {
			dataMap.put(currentNodeName, new StringBuilder().append(dataMap.get(currentNodeName)).append(new String(chars, start, length)).toString());
		} else {
			dataMap.put(currentNodeName, new String(chars, start, length));
		}
		
	}
}