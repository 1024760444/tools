package main;

import java.util.List;

import com.yhaitao.dlock.ZooKeeperUtils;

public class ZooKeeperUtilsTest {
	public static void main(String[] args) {
		try {
			ZooKeeperUtils zkUtils = new ZooKeeperUtils("172.19.10.5:2181");
			String nodeName = zkUtils.createEs("/rmi/yht", "123");
			System.err.println(nodeName);
			
			String nodeName2 = zkUtils.createEs("/rmi/yht", "123");
			System.err.println(nodeName2);
			
			String nodeName3 = zkUtils.createEs("/rmi/yht", "123");
			System.err.println(nodeName3);
			
			List<String> children = zkUtils.getChildren("/rmi");
			for(String child : children) {
				System.out.print(child + " "); 
			}
			
			Thread.sleep(20000);
			
			zkUtils.close();
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
