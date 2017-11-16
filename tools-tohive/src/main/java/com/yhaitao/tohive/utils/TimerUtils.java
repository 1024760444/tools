package com.yhaitao.tohive.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * 时间戳处理。
 * @author yhaitao
 *
 */
public class TimerUtils {
	/**
	 * 获取当前系统时间戳。 格式为： yyyyMMddHHmmssSSS 。
	 * @return 当前系统时间戳
	 */
	public static String getSystemTime() {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		return format.format(Calendar.getInstance().getTime());
	}
}
