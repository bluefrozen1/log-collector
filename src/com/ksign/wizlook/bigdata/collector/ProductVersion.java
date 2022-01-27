package com.ksign.wizlook.bigdata.collector;

/**
 * 제품 버전명 저장 클래스
 * @author byw
 */
public class ProductVersion {
	private static String PRODUCT_VERSION;

	public static String getProductVertion() {
		return PRODUCT_VERSION;
	}

	public static void setProductVersion(String productVersion) {
		ProductVersion.PRODUCT_VERSION = productVersion;
	}
}