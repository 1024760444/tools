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
	public final static String ZK_DLOCK_PATH = "/dlock";
	
	/**
	 * 创建分布式锁基础路径。
	 */
	public final static String ZK_DLOCK_KEY = "/key";
}
