package com.yhaitao.dlock;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.yhaitao.dlock.utils.Constant;

/**
 * 分布式锁。基于ZK实现。
 * @author yhaitao
 *
 */
public class ZooKeeperUtils {
	/**
	 * 连接创建同步工具类
	 */
	private CountDownLatch latch = new CountDownLatch(1);
	
	/**
	 * ZK连接
	 */
	private ZooKeeper zooKeeper;
	private String zkHosts;
	
	/**
	 * 连接ZK服务器。
	 * @param zkHosts zk服务器列表
	 * @return ZK连接
	 * @throws Exception 
	 */
	public ZooKeeperUtils(String zkHosts) throws Exception {
		this(zkHosts, Constant.ZK_CONN_TIMEOUT);
	}

	/**
	 * 连接ZK服务器。
	 * @param zkHosts zk服务器列表
	 * @param timeOut 连接超时时间
	 * @return ZK连接
	 * @throws Exception 
	 */
	public ZooKeeperUtils(String zkHosts, int timeOut) throws Exception {
		this.zkHosts = zkHosts;
		try {
			// 创建ZK连接
			this.zooKeeper = new ZooKeeper(zkHosts, timeOut, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if(event.getState() == Event.KeeperState.SyncConnected) {
						latch.countDown();
					}
				}
			});
			
			// 等待创建成功后再返回
			latch.await();
		} catch (Exception e) {
			throw new Exception(eMessage("ZooKeeperUtils", null, e.getMessage()));
		}
	}
	
	/**
	 * 创建PERSISTENT节点，先判断该节点是否存在；如果不存在就创建该节点。不判断父类节点是否存在，如果父类节点不存在则创建失败。
	 * @param path 节点目录
	 * @param data 保持到目录的数据
	 * @throws Exception 
	 */
	public void createIfNotExists(String path, String data) throws Exception {
		try {
			Stat pathStat = this.zooKeeper.exists(path, false);
			if (pathStat == null) {
				this.zooKeeper.create(path, 
						data == null ? null : data.getBytes(), 
						ZooDefs.Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			throw new Exception(eMessage("createIfNotExists", path, e.getMessage()));
		}
	}
	
	/**
	 * 创建EPHEMERAL_SEQUENTIAL节点。
	 * @param path 节点目录
	 * @param data 保持到目录的数据
	 * @return 创建节点名称
	 * @throws Exception
	 */
	public String createEs(String path, String data) throws Exception {
		try {
			return this.zooKeeper.create(path, 
					data == null ? null : data.getBytes(), 
					ZooDefs.Ids.OPEN_ACL_UNSAFE, 
					CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (Exception e) {
			throw new Exception(eMessage("createEs", path, e.getMessage()));
		}
	}
	
	/**
	 * 获取目录子节点列表。
	 * @param path 父目录
	 * @return 指定目录的子节点名称列表
	 * @throws Exception
	 */
	public List<String> getChildren(String path) throws Exception {
		try {
			return this.zooKeeper.getChildren(path, false);
		} catch (KeeperException e) {
			throw new Exception(eMessage("getChildren", path, e.getMessage()));
		}
	}
	
	/**
	 * 获取指定节点的数据，并监控该节点。
	 * @param path 获取数据的目录
	 * @param watcher 监控节点
	 * @return 数据
	 * @throws Exception
	 */
	public byte[] getData(String path, Watcher watcher) throws Exception {
		try {
			return this.zooKeeper.getData(path, watcher, new Stat());
		} catch (Exception e) {
			throw new Exception(eMessage("getData", path, e.getMessage()));
		}
	}
	
	/**
	 * 删除节点。
	 * @param path 需要删除的节点
	 * @throws Exception
	 */
	public void delete(String path) throws Exception {
		try {
			this.zooKeeper.delete(path, -1);
		} catch (Exception e) {
			throw new Exception(eMessage("delete", path, e.getMessage()));
		}
	}
	
	/**
	 * 关闭ZooKeeper连接
	 * @param zooKeeper 需要关闭的ZooKeeper连接
	 */
	public void close() throws Exception {
		if(this.zooKeeper != null) {
			try {
				this.zooKeeper.close();
			} catch (InterruptedException e) {
				throw new Exception(eMessage("close", null, e.getMessage()));
			}
		}
	}
	
	/**
	 * 异常信息组合。
	 * @param method 抛出异常的方法
	 * @param e 异常对象
	 * @return 需要抛出的异常信息
	 */
	protected String eMessage(String method, String path, String eMsg) {
		StringBuffer sb = new StringBuffer();
		sb.append("ZooKeeper ").append(this.zkHosts).append(", ").append(method);
		if(path != null) {
			sb.append(" ").append(path);
		}
		sb.append(", Exception ").append(eMsg);
		return sb.toString();
	}
}
