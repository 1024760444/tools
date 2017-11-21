package com.yhaitao.dlock.utils;

/**
 * 静态变量定义。
 * @author admin
 *
 */
public class Constant {
	/**
	 * 连接ZK的超时时间
	 */
	public final static int ZK_CONN_TIMEOUT = 6000;
	
	/**
	 * 创建分布式锁基础路径。
	 */
	public final static String ZK_DLOCK_ROOT = "/disLock";
	
	/**
	 * 创建分布式锁基础路径。
	 */
	public final static String ZK_DLOCK_KEY = "key";
	
	/**
	 * 判断指定字符串是否为空或者为null。
	 * @param str 输入字符串
	 * @return 为null或者为空返回true，否则返回false
	 */
	public static boolean isNotNull(String str) {
		return str != null && !"".equals(str);
	}
}
