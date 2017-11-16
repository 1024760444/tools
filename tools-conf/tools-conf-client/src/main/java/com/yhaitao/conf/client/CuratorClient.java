package com.yhaitao.conf.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.conf.client.utils.ToolsUtils;

/**
 * 提供向ZooKeeper写数据的方法。
 * @author yanghaitao
 *
 */
public class CuratorClient {
	/**
	 * 日志对象
	 */
	private static Logger LOGGER = LoggerFactory.getLogger(CuratorClient.class);
	
	/**
	 * Curator客户端
	 */
	private CuratorFramework curator;
	
	/**
	 * 初始化客户端。
	 * @param zkList ZooKeeper服务器列表。例如：127.0.0.1:2181
	 */
	public CuratorClient(String zkList) {
		/** 1 链接ZooKeeper **/
		curator = CuratorFrameworkFactory.builder()
				.connectString(zkList)
				.sessionTimeoutMs(30000).connectionTimeoutMs(30000)
				.canBeReadOnly(false)
				.retryPolicy(new ExponentialBackoffRetry(1000, Integer.MAX_VALUE))
				.namespace(ToolsUtils.NAMESPACE)
				.defaultData(null)
				.build();
		curator.start();
		LOGGER.info("start connectString : {}, namespace : {}. ", zkList, ToolsUtils.NAMESPACE);
	}
	
	/**
	 * 向指定路径写数据。
	 * @param path 需要些数据的路径。 路径不存在，则创建。
	 * @param data 需要写入的数据。
	 * @throws Exception 
	 */
	public void write(String path, byte[] data) throws Exception {
		/** 初始化需要设置的路径 **/
		Stat forPath = this.curator.checkExists().forPath(path);
		if(null == forPath) {
			initConfigPath(path);
		}
		/** 设置数据 **/
		this.curator.setData().forPath(path, data);
	}
	
	/**
	 * 初始化创建原始路径
	 */
	private void initConfigPath(String fullPath) {
		/** 拆分需要创建的路径 **/
		String[] pathArray = null; 
		try {
			pathArray = null == fullPath ? null : fullPath.split("\\/");
		} catch (Exception e) {
			LOGGER.info("initConfigPath configPath : {}, Exception : {}. ", fullPath, e.getMessage());
		}
		if(null == pathArray) {
			return ;
		}
		
		/** 分别创建路径 **/
		String createPath = null;
		for(String path : pathArray) {
			createPath = null == createPath ? path : createPath + "/" + path;
			try {
				Stat forPath = this.curator.checkExists().forPath(createPath);
				if(null == forPath) {
					this.curator.create().forPath(createPath);
				}
			} catch (Exception e) {
				LOGGER.info("initConfigPath create path : {}, Exception : {}. ", createPath, e.getMessage());
			}
		}
	}
	
	/**
	 * 关闭写数据连接。
	 */
	public void close() {
		this.curator.close();
	}
}
