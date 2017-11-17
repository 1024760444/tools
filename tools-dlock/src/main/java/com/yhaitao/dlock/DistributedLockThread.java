package com.yhaitao.dlock;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.dlock.utils.Constant;

/**
 * 存在分布式锁的线程。
 * @author yhaitao
 *
 */
public abstract class DistributedLockThread extends ZooKeeperUtils implements Runnable {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(DistributedLockThread.class);
	
	/**
	 * 所集合路径
	 */
	private String lockPath;
	
	/**
	 * 分布式线程的创建。
	 * @param zkHosts ZK地址
	 * @param path 锁路径
	 * @throws Exception 建立ZK连接或者创建锁失败
	 */
	public DistributedLockThread(String zkHosts, String path) throws Exception {
		super(zkHosts);
		// 基础目录， 锁目录
		this.lockPath = Constant.ZK_DLOCK_PATH + "/" + path;
		createIfNotExists(Constant.ZK_DLOCK_PATH, null);
		createIfNotExists(this.lockPath, null);
	}
	
	@Override
	public void run() {
		// 创建 EPHEMERAL_SEQUENTIAL 节点
		try {
			String selfNode = createEs(this.lockPath + "/" + Constant.ZK_DLOCK_KEY, null);
			boolean hasLock = getLock(selfNode);
			
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}
	
	/**
	 * 获取锁。
	 * @param selfNode 当前节点名称
	 * @return 获得锁返回true，未获得锁返回false。
	 * @throws Exception 
	 */
	private boolean getLock(String selfNode) throws Exception {
		List<String> children = this.getChildren(this.lockPath);
		Collections.sort(children);
		int selfIndex = children.indexOf(selfNode);
		switch(selfIndex) {
			case -1 : {
				throw new Exception(eMessage("getLock", selfNode, "selfNode not exist"));
			}
			case 0 : {
				return true;
			}
			default : {
				
				break;
			}
		}
		return false;
	}
}
