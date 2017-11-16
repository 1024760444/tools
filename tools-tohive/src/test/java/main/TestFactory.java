package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import com.yhaitao.tohive.ToHiveInputSplit;

public class TestFactory {
	public static void main(String[] args) {
		String path = "E:/workspace2/tools-tohive/src/main/resources/";
		Configuration conf = new Configuration();
		conf.addResource(new Path(path + "core-site.xml"));
		conf.addResource(new Path(path + "hdfs-site.xml"));
		conf.addResource(new Path(path + "mapred-site.xml"));
		conf.addResource(new Path(path + "yarn-site.xml"));
		conf.addResource(new Path(path + "tools-tohive.xml"));
		SerializationFactory factory = new SerializationFactory(conf);
		Serializer<?> serializer = factory.getSerializer(ToHiveInputSplit.class);
		
		if(serializer == null) {
			System.err.println("serializer is null ");
		} else {
			System.err.println("serializer is not ");
		}
	}
}
