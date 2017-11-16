package com.yhaitao.conf.client;

import java.util.Iterator;
import java.util.Properties;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.conf.client.utils.ToolsUtils;

/**
 * 统一配置客户端。
 * @author yanghaitao
 *
 */
public class ConfigClient {
	/**
	 * 日志对象
	 */
	private static Logger LOGGER = LoggerFactory.getLogger(ConfigClient.class);
	
	/**
	 * ZK集群地址
	 */
	private String zkList;
	
	/**
	 * 配置路径
	 */
	private String configPath;
	
	/**
	 * 统一配置变动监听
	 */
	private ConfigClientLisenter configLisenter;
	
	/**
	 * 本地配置
	 */
	private Properties localProperties = new Properties();
	
	/**
	 * Curator客户端
	 */
	private CuratorFramework curator;
	
	/**
	 * 数据监听器定义
	 */
	private Watcher dataChangedWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			if(event.getType() == EventType.NodeDataChanged) {
				nodeDataChanged();
			}
		}
	};
	
	/**
	 * 启动统一配置。
	 */
	public void start() {
		/** 1 链接ZooKeeper **/
		curator = CuratorFrameworkFactory.builder()
				.connectString(this.getZkList())
				.sessionTimeoutMs(30000).connectionTimeoutMs(30000)
				.canBeReadOnly(false)
				.retryPolicy(new ExponentialBackoffRetry(1000, Integer.MAX_VALUE))
				.namespace(ToolsUtils.NAMESPACE)
				.defaultData(null)
				.build();
		LOGGER.info("start connectString : {}, namespace : {}. ", this.getZkList(), ToolsUtils.NAMESPACE);
		
		curator.start();
		LOGGER.info("start CuratorFramework start ... ");
		
		/** 2 初始化创建原始路径 **/
		initConfigPath();
		
		/** 3 监听数据的变动 **/
		nodeDataChanged();
		
		/** 4 永久监听数据的变动 **/
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (Exception e) {
			LOGGER.info("start Exception. zkList : {}, configPath : {}, Exception : {}. ", 
					this.getZkList(), this.getConfigPath(), e.getMessage());
			curator.close();
		}
	}
	
	/**
	 * 初始化创建原始路径
	 */
	private void initConfigPath() {
		/** 拆分需要创建的路径 **/
		String[] pathArray = null; 
		try {
			pathArray = null == this.getConfigPath() ? null : this.getConfigPath().split("\\/");
		} catch (Exception e) {
			LOGGER.info("initConfigPath configPath : {}, Exception : {}. ", this.getConfigPath(), e.getMessage());
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
	 * 节点数据变动。
	 */
	private void nodeDataChanged() {
		LOGGER.info("nodeDataChanged zkList : {}, configPath : {}. ", this.getZkList(), this.getConfigPath());
		try {
			/** 监听并获取数据 **/
			byte[] data = curator.getData()
					.usingWatcher(dataChangedWatcher)
					.forPath(this.getConfigPath());
			/** 解析数据，调用配置监听 **/
			if(data != null) {
				Properties properties = ToolsUtils.bytes2Properties(data);
				configLisenter(properties);
			} else {
				LOGGER.info("nodeDataChanged getData null from zkList : {}, configPath : {}. ", 
						this.getZkList(), this.getConfigPath());
			}
		} catch (Exception e) {
			LOGGER.info("nodeDataChanged getData Exception zkList : {}, configPath : {}, Exception : {}. ", 
					this.getZkList(), this.getConfigPath(), e.getMessage());
		}
	}
	
	/**
	 * 配置监听。
	 */
	private void configLisenter(Properties properties) {
		/** 参数变更：新增和修改 **/
		Iterator<Object> newIterator = properties.keySet().iterator();
		while(newIterator.hasNext()) {
			String key = (String) newIterator.next();
			String newValue = properties.getProperty(key);
			/** 新增参数的监听 **/
			if(!this.localProperties.containsKey(key)) {
				this.localProperties.setProperty(key, newValue);
				if(this.configLisenter != null) {
					this.configLisenter.add(key, newValue);
				}
			} 
			/** 修改参数的监听 **/
			else {
				String oldValue = this.localProperties.getProperty(key);
				if(!oldValue.equals(newValue)) {
					this.localProperties.setProperty(key, newValue);
					if(this.configLisenter != null) {
						this.configLisenter.update(key, oldValue, newValue);
					}
				}
			}
		}
		/** 参数变更：删除 **/
		if(!this.localProperties.isEmpty()) {
			Iterator<Object> oldIterator = this.localProperties.keySet().iterator();
			while(oldIterator.hasNext()) {
				String key = (String) oldIterator.next();
				if(!properties.containsKey(key)) {
					String value = localProperties.getProperty(key);
					oldIterator.remove();
					if(this.configLisenter != null) {
						this.configLisenter.remove(key, value);
					}
				}
			}
		}
	}
	
	/**
	 * 获取监听的ZK地址。
	 * @return 监听的ZK地址
	 */
	public String getZkList() {
		return zkList;
	}
	
	/**
	 * 设置监听的ZK地址。
	 * @param zkList 监听的ZK地址。例如：127.0.0.1:2181
	 */
	public void setZkList(String zkList) {
		this.zkList = zkList;
	}
	
	/**
	 * 获取监听的配置路径。
	 * @return 监听的配置路径
	 */
	public String getConfigPath() {
		return configPath;
	}
	
	/**
	 * 设置监听的配置路径。
	 * @param configPath 监听的配置路径
	 */
	public void setConfigPath(String configPath) {
		this.configPath = configPath;
	}
	
	/**
	 * 获取监听动作。
	 * @return 监听动作
	 */
	public ConfigClientLisenter getConfigLisenter() {
		return configLisenter;
	}
	
	/**
	 * 设置监听动作。
	 * @param configLisenter 监听动作
	 */
	public void setConfigLisenter(ConfigClientLisenter configLisenter) {
		this.configLisenter = configLisenter;
	}
}
