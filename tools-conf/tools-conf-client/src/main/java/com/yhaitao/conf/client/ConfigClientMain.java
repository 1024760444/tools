package com.yhaitao.conf.client;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.conf.client.utils.ToolsUtils;

/**
 * 向zookeeper推送数据。
 * @author yanghaitao
 *
 */
public class ConfigClientMain {
	/**
	 * 日志对象
	 */
	private static Logger LOGGER = LoggerFactory.getLogger(ConfigClientMain.class);
	
	/**
	 * 数据推送入口。
	 * 需要三个参数：zkList，zk数据路径，需要推送的properties文件路径。
	 * @param args 输入参数
	 */
	public static void main(String[] args) {
		/** 参数个数校验 **/
		LOGGER.info("input args : {}.", null == args ? "null" : ToolsUtils.GSON.toJson(args));
		if(null == args || args.length < 3) {
			LOGGER.error("input args error! zkList : 127.0.0.1:2181, zkpath : test/yang, properties path : D:/yanghaitao/conf/rms.properties");
			return ;
		}
		
		/** 加载配置文件 **/
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(new File(args[2])));
			LOGGER.info("properties : {}.", null == args ? "null" : ToolsUtils.GSON.toJson(properties));
		} catch (Exception e) {
			LOGGER.error("properties load path : {}, Exception : {}.", args[2], e.getMessage());
		}
		
		/** 数据转换 **/
		byte[] bytes = ToolsUtils.properties2Bytes(properties);
		LOGGER.info("bytes : {}.", null == bytes ? "null" : bytes.length);
		
		/** 向zookeeper写入数据 **/
		if(null != bytes) {
			try {
				CuratorClient curatorClient = new CuratorClient(args[0]);
				curatorClient.write(args[1], bytes);
				curatorClient.close();
				LOGGER.info("curatorClient write success zkList : {}, zkPath : {}.", args[0], args[1]);
			} catch (Exception e) {
				LOGGER.error("curatorClient write zkList : {}, zkPath : {}, Exception : {}.", args[0], args[1], e.getMessage());
			}
		}
		LOGGER.info("------------------ over ------------------");
	}
}
