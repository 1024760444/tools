package com.yhaitao.dlock;

/**
 * 需要执行的分布式任务。
 * @author yhaitao
 *
 */
public interface DistributedTask {
	/**
	 * 任务内容。
	 */
	public void task();
}
