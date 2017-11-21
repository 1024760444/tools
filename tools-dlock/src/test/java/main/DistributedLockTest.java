package main;

import java.util.concurrent.CountDownLatch;

import com.yhaitao.dlock.DistributedLock;
import com.yhaitao.dlock.DistributedTask;

public class DistributedLockTest {
	public static void main(String[] args) throws Exception {
		for(int i = 0; i < 10; i++) {
			new Thread() {
				public void run() {
					try {
						test();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
		}
	}
	
	public static void test() throws Exception {
		// 本地同步锁，任务执行完成后，才结束；以等待zk。
		final CountDownLatch latch = new CountDownLatch(1);
		
		// 需要执行的任务
		DistributedTask task = new DistributedTask() {
			@Override
			public void task() {
				System.err.println(Thread.currentThread().getName() + " : " + System.currentTimeMillis());
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				latch.countDown();
			}
		};
		
		// 创建分布式锁
		DistributedLock dlock = new DistributedLock("172.19.10.5:2181", null, task);
		boolean lock = dlock.lock();
		if(lock) {
			task.task();
			dlock.unLock();
		}
		latch.await();
	}
}
