package com.yhaitao.conf.client.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.gson.Gson;

/**
 * 统一配置工具类。
 * @author yanghaitao
 *
 */
public class ToolsUtils {
	/**
	 * 统一配置的命名空间。
	 */
	public static final String NAMESPACE = "config_client";
	
	/**
	 * Gson字符串转换
	 */
	public static final Gson GSON = new Gson();

	/**
	 * Properties文件转化为byte数据
	 * @param properties Properties文件
	 * @return byte数据
	 */
	public static byte[] properties2Bytes(Properties properties) {
		/** Properties to Map **/
		Iterator<Entry<Object, Object>> iterator = properties.entrySet().iterator();
		Map<Object, Object> dataMap = new HashMap<Object, Object>();
		while(iterator.hasNext()) {
			Entry<Object, Object> next = iterator.next();
			Object key = next.getKey();
			Object value = next.getValue();
			if(null != key) {
				dataMap.put(key, value);
			}
		}
		/** Map to json String and to byte **/
		String json = GSON.toJson(dataMap);
		return null == json ? null : json.getBytes();
	}
	
	/**
	 * 数据转换为Properties文件
	 * @param bytes byte数据
	 * @return Properties文件
	 */
	@SuppressWarnings("unchecked")
	public static Properties bytes2Properties(byte[] bytes) {
		Properties properties = new Properties();
		if(null != bytes) {
			String string = new String(bytes);
			Map<Object, Object> dataMap = GSON.fromJson(string, Map.class);
			properties.putAll(dataMap);
		}
		return properties;
	}
}
