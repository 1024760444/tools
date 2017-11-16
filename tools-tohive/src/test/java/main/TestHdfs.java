package main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHdfs {
	public static void main(String[] args) throws IOException {
		String path = "E:/workspace2/tools-tohive/src/main/resources/";
		Configuration conf = new Configuration();
		conf.addResource(new Path(path + "core-site.xml"));
		conf.addResource(new Path(path + "hdfs-site.xml"));
		conf.addResource(new Path(path + "mapred-site.xml"));
		conf.addResource(new Path(path + "yarn-site.xml"));
		conf.addResource(new Path(path + "tools-tohive.xml"));
		
		FileSystem fileSystem = FileSystem.get(conf);
		Path makeQualified = fileSystem.makeQualified(new Path("/tmp/tohive"));
		System.err.println(makeQualified);
	}
	
	public static void test() {
		String path = "E:/workspace2/tools-tohive/src/main/resources/";
		Configuration conf = new Configuration();
		conf.addResource(new Path(path + "core-site.xml"));
		conf.addResource(new Path(path + "hdfs-site.xml"));
		conf.addResource(new Path(path + "mapred-site.xml"));
		conf.addResource(new Path(path + "yarn-site.xml"));
		conf.addResource(new Path(path + "tools-tohive.xml"));
		
		// 67108864
		StringBuffer sb = new StringBuffer();
		for(int i = 0; ; i++) {
			sb.append(i).append("\001viewport\001life.hao123.com\00113\0012.00\0012017-08-18 21:26:23").append("\n");
			if(sb.length() >= 67108864) {
				break;
			}
		}
		
		String hive_Path = "/user/hive/warehouse/ichart.db/t_mark_chinaz_words/psuperid=13";
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			boolean exists = fileSystem.exists(new Path(hive_Path));
			if(!exists) {
				fileSystem.mkdirs(new Path(hive_Path));
			}
			
			FSDataOutputStream output = fileSystem.create(new Path(hive_Path + "/20170824003521.txt"));
			output.writeBytes(sb.toString());
			output.flush();
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
