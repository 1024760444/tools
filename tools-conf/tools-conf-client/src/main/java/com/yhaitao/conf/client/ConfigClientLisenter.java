package com.yhaitao.conf.client;

/**
 * 统一配置开发的参数设置接口。
 * @author yanghaitao
 *
 */
public interface ConfigClientLisenter {
	/**
	 * 新增的参数
	 * @param key 新增键
	 * @param value 新增值
	 */
	public void add(String key, String value);
	
	/**
	 * 删除的参数
	 * @param key 删除键
	 * @param value 删除值
	 */
	public void remove(String key, String value);
	
	/**
	 * 修改参数
	 * @param key 修改键
	 * @param oldValue 以前的键值
	 * @param newValue 现在的键值
	 */
	public void update(String key, String oldValue, String newValue);
}
