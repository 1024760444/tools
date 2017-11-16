package com.yhaitao.common.utils;

/**
 * 字符串处理工具。
 * @author yhaitao
 *
 */
public class StringUtils {
	/**
	 * 指定输入字符串是否包含英文字符。
	 * @param string 待检测字符串
	 * @return 包含英文字符，返回true；否则返回false。
	 */
	public static boolean hasEn(String string) {
		int length = string.length();
		for(int index = 0; index < length; index++) {
			char key = string.charAt(index);
			if(key >= 0 && key <= 127) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 判断输入字符串是否为空。
	 * @param text 待判定字符串
	 * @return 为空或者未null，返回true；否则返回false。
	 */
	public static boolean isNull(String text) {
		return (null == text || "".equals(text)) ? true : false;
	}
}
