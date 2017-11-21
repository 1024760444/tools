package com.yhaitao.dlock;

import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.dlock.utils.Constant;

/**
 * 分布式锁对象。
 * <p>3、ZK分布式锁实现
 * <p>（1）创建共同的锁目录Znode节点。例如：/lockRoot。
 * <p>（2）注册任务，创建一个有序临时节点获取节点名称。例如：/lockRoot/lock00000001,/lockRoot/lock00000002。
 * <p>（3）获取锁目录/lockRoot所有子节点名称列表。
 * <p>（4）如果当前节点名称为最小节点，获得锁 —— 执行任务。
 * <p>（5）如果当前节点名称不为最小节点，使用Watcher监控当前节点的前面一个节点。
 * <p>（6）当Watcher被触发时，从第（3）步开始执行。
 * @author yhaitao
 */
public final class DistributedLock extends ZooKeeperUtils implements Watcher {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(DistributedLock.class);
	
	/**
	 * 当前锁目录
	 */
	private String lockRoot;
	
	/**
	 * 当前注册的节点名称
	 */
	private String currentZNodeName;
	
	/**
	 * 需要执行的分布式任务。
	 */
	private DistributedTask task;
	
	/**
	 * 初始化分布式锁。
	 * @param zkHosts ZK集群列表
	 * @param lockRoot 共同锁目录
	 * @throws Exception 锁初始化失败
	 */
	public DistributedLock(String zkHosts, String lockRoot, DistributedTask task) throws Exception {
		super(zkHosts);
		
		// 设置默认的锁目录结构
		this.lockRoot = Constant.isNotNull(lockRoot) ? (Constant.ZK_DLOCK_ROOT + "/" + lockRoot)
				: (Constant.ZK_DLOCK_ROOT + "/lockRoot");
		if(task == null) {
			throw new Exception(eMessage("DistributedLock", null, "Distributed Task is null"));
		} else {
			this.task = task;
		}
		
		// 创建共同的锁目录Znode节点。
		this.createIfNotExists(Constant.ZK_DLOCK_ROOT, null);
		this.createIfNotExists(this.lockRoot, null);
		
		// 创建当前注册节点，获取节点名称
		String selfPath = this.createEs(this.lockRoot + "/" + Constant.ZK_DLOCK_KEY, null);
		this.currentZNodeName = selfPath.substring(this.lockRoot.length() + 1);
	}
	
	/**
	 * 获取锁。
	 * @return 获取锁成功返回true，未获得锁返回false。
	 * @throws Exception 
	 */
	public boolean lock() throws Exception {
		// 获取锁目录/lockRoot所有子节点名称列表。
		List<String> children = this.getChildren(this.lockRoot);
		Collections.sort(children);
		
		int indexOf = children.indexOf(this.currentZNodeName);
		switch (indexOf) {
			// 不存在当前节点
			case -1: {
				LOGGER.info("CurrentNode : {}, not found!", this.currentZNodeName);
				throw new Exception(eMessage("getLock", this.lockRoot + "/" + this.currentZNodeName, "not found"));
			}
			// 当前节点是，最小节点；获得锁
			case 0: {
				LOGGER.info("CurrentNode : {}, getLock!", this.currentZNodeName);
				return true;
			}
			default: {
				String preNodeName = children.get(indexOf - 1);
				try {
					getData(this.lockRoot + "/" + preNodeName, this);
					LOGGER.info("PreviousNode : {}, be watchered! ", preNodeName);
					return false;
				} catch (Exception e) {
					LOGGER.info("PreviousNode : {}, not found! GetLock again!", preNodeName);
					// 获取前面节点失败
					return lock();
				}
			}
		}
	}
	
	/**
	 * 释放当前锁，同时关闭zk。
	 * @throws Exception 
	 */
	public void unLock() throws Exception {
		try {
			delete(this.lockRoot + "/" + this.currentZNodeName);
			close();
		} catch (Exception e) {
			throw new Exception(eMessage("unLock", null, e.getMessage()));
		}
	}
	
	/**
	 * 节点的监控执行。
	 */
	@Override
	public void process(WatchedEvent event) {
		if(event.getType() == Event.EventType.NodeDeleted) {
			try {
				boolean lock = this.lock();
				if(lock) {
					this.task.task();
					this.unLock();
				}
			} catch (Exception e) {
				LOGGER.info(eMessage("process", null, e.getMessage()));
			}
		}
	}
}
