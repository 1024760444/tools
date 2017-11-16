package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.yhaitao.tohive.utils.Common;

public class TestFileSystem {
	public static void main(String[] args) {
		// 指定文件夹
		String path = "E:/github/tools-tohive/src/main/resources";
		Configuration conf = new Configuration();
		conf.addResource(new Path(path + "/core-site.xml"));
		conf.addResource(new Path(path + "/hdfs-site.xml"));
		conf.addResource(new Path(path + "/mapred-site.xml"));
		conf.addResource(new Path(path + "/yarn-site.xml"));
		conf.addResource(new Path(path + "/tools-tohive.xml"));
		String hdfs_ext_jars = conf.get(Common.KEY_HDFS_PATH_EXT_JARS);
		
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(hdfs_ext_jars), false);
			while(listFiles.hasNext()) {
				LocatedFileStatus next = listFiles.next();
				String name = next.getPath().toUri().getPath();
				System.err.println(name);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
