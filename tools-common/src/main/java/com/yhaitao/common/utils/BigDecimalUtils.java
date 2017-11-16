package com.yhaitao.common.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 数据处理公共方法。
 * @author yhaitao
 *
 */
public class BigDecimalUtils {
	/**
	 * 数字归一化。结果保留四位小数。
	 * @param data 数字
	 * @return 归一化结果
	 */
	public static double normalization(double data) {
		return normalization(data, 4);
	}
	
	/**
	 * 数字归一化。
	 * @param data 数字
	 * @param size 保留小数位数
	 * @return 归一化结果
	 */
	public static double normalization(double data, int size) {
		double normal = 0;
		if(data > 1) {
			normal = 1 - (double) 1 / data;
		}
		BigDecimal bg = new BigDecimal(normal).setScale(size, RoundingMode.UP);
		return bg.doubleValue();
	}
	
	/**
	 * 对输入数据保留指定位数的值返回。四舍五入。
	 * @param input 输入数据
	 * @param digit 需要保留位数
	 * @return 最多指定位数的小数
	 */
	public static double retain(double input, int digit) {
		BigDecimal bigDecimal = new BigDecimal(input);
		return bigDecimal.setScale(digit, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
	/**
	 * 四舍五入，最多保留四位小数。
	 * @param input 输入数据
	 * @return 最多保留四位小数
	 */
	public static double retainMaxF(double input) {
		return retain(input, 4);
	}
}
