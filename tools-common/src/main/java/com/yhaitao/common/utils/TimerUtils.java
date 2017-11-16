package com.yhaitao.common.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 时间戳处理工具
 * @author yhaitao
 *
 */
public class TimerUtils {
	/**
	 * 时间格式
	 */
	private final static String PATTERN = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * 获取系统时间。 时间格式： yyyy-MM-dd HH:mm:ss
	 * @return 系统时间
	 */
	public static String getSystemDate() {
		SimpleDateFormat format = new SimpleDateFormat(PATTERN);
		return format.format(Calendar.getInstance().getTime());
	}
}
