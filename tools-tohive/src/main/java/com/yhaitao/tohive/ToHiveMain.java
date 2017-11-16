package com.yhaitao.tohive;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.tohive.mapper.MultipleOutputsMapper;
import com.yhaitao.tohive.reducer.MultipleOutputsReducer;
import com.yhaitao.tohive.utils.Common;
import com.yhaitao.tohive.utils.TimerUtils;

/**
 * 关系型数据转换到Hive中。
 * @author yhaitao
 *
 */
public class ToHiveMain {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(ToHiveMain.class);
	
	/**
	 * 数据转换入口。
	 * @param args 入参数组。  tools-tohive.xml路径， hadoop配置路径， hive配置文件路径
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// 输入配置文件路径
		if(null == args || args.length < 1) {
			LOGGER.error("Over, Input Conf Path! ");
			return ;
		}
		
		// 检验配置文件路径
		String confPath = args[0];
		if(!hasConf(confPath)) {
			LOGGER.error("Over, Configuration file miss... ");
			return ;
		}
		
		// 配置文件读取
		Configuration conf = new Configuration();
		conf.addResource(new Path(confPath + "/tools-tohive.xml"));
		conf.addResource(new Path(confPath + "/core-site.xml"));
		conf.addResource(new Path(confPath + "/hdfs-site.xml"));
		conf.addResource(new Path(confPath + "/mapred-site.xml"));
		conf.addResource(new Path(confPath + "/yarn-site.xml"));
		// conf.set("yarn.app.mapreduce.am.staging-dir", "/tmp/tohive/staging");
		// mapreduce.job.submithostname=PC-20140709BEUI, mapreduce.job.submithostaddress=10.0.18.148
		// conf.set("mapreduce.job.submithostname", "hdp1");
		// conf.set("mapreduce.job.submithostaddress", "192.168.124.128");
		
		// 参数校验
		if(!checkConf(conf)) {
			LOGGER.error("Over, Configuration params miss... ");
			return ;
		}
		
		// 提交任务
		try {
			createAndSubmitTask(conf);
		} catch (Exception e) {
			LOGGER.error("Over, hiveTask Exception : {}. ", e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	/**
	 * 创建提交任务MR任务
	 * @param conf
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private static void createAndSubmitTask(Configuration conf) throws Exception {
		// 创建任务
		String jobName = "toHive-" + TimerUtils.getSystemTime();
		Job hiveJob = Job.getInstance(conf, jobName);
		hiveJob.setMapperClass(MultipleOutputsMapper.class);
		hiveJob.setMapOutputKeyClass(Text.class);  
		hiveJob.setMapOutputValueClass(Text.class);
		
		hiveJob.setReducerClass(MultipleOutputsReducer.class);
		hiveJob.setOutputKeyClass(NullWritable.class);
		hiveJob.setOutputValueClass(Text.class);
		
		int headNum = conf.getInt(Common.KEY_JDBC_MAP_NUMBER, 4);
		hiveJob.setNumReduceTasks(headNum);
		hiveJob.setJarByClass(ToHiveMain.class);
		
		// 加载额外引入的jar包，这些jar包需要上传到hdfs文件系统中
		List<String> jarList = getInputJars(conf);
		if(jarList != null) {
			for(String jarPath : jarList) {
				if(StringUtils.isNotBlank(jarPath) && jarPath.endsWith(".jar")) {
					hiveJob.addFileToClassPath(new Path(jarPath.trim()));
				}
			}
		}
		
		// input and output
		Path outputPath = new Path(conf.get(Common.KEY_HIVE_DST_TMP_PATH));
		hiveJob.setInputFormatClass(ToHiveInputFormat.class);
		hiveJob.setOutputFormatClass(NoChkOutputFileOutputFormat.class);
		NoChkOutputFileOutputFormat.setOutputPath(hiveJob, outputPath);
		
		// 提交并等待任务
		hiveJob.waitForCompletion(true);
		LOGGER.info("Job : {}, Over... ", jobName);
	}
	
	/**
	 * 参数校验是否通过。
	 * @param conf 参数配置
	 * @return 校验通过返回true，否则返回false。
	 */
	private static boolean checkConf(Configuration conf) {
		return true;
	}
	
	/**
	 * 获取输入Jar列表
	 * @param conf 配置
	 * @return Jar列表
	 * @throws Exception 
	 */
	private static List<String> getInputJars(Configuration conf) throws Exception {
		// 指定文件夹
		List<String> jarList = new ArrayList<String>();
		String hdfs_ext_jars = conf.get(Common.KEY_HDFS_PATH_EXT_JARS);
		if(StringUtils.isBlank(hdfs_ext_jars)) {
			return jarList;
		}
		
		// 读取hdfs文件，获取jar列表
		FileSystem fileSystem = FileSystem.get(conf);
		RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(hdfs_ext_jars), false);
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			String name = next.getPath().toUri().getPath();
			jarList.add(name);
		}
		return jarList;
	}
	
	/**
	 * 所需配置文件是否都存在。路径如： /usr/local/hadoop/etc/hadoop
	 * 所需配置文件列表： 
	 * tools-tohive.xml
	 * core-site.xml
	 * hdfs-site.xml
	 * mapred-site.xml
	 * yarn-site.xml
	 * @param confPath 配置文件路径
	 * @return 都存在返回true，否则返回false。
	 */
	private static boolean hasConf(String confPath) {
		// tools-tohive.xml
		if(!new File(confPath + "/tools-tohive.xml").isFile()) {
			LOGGER.error("tools-tohive.xml is miss... ");
			return false;
		}
		// core-site.xml
		if(!new File(confPath + "/core-site.xml").isFile()) {
			LOGGER.error("core-site.xml is miss... ");
			return false;
		}
		// hdfs-site.xml
		if(!new File(confPath + "/hdfs-site.xml").isFile()) {
			LOGGER.error("hdfs-site.xml is miss... ");
			return false;
		}
		// mapred-site.xml
		if(!new File(confPath + "/mapred-site.xml").isFile()) {
			LOGGER.error("mapred-site.xml is miss... ");
			return false;
		}
		// yarn-site.xml
		if(!new File(confPath + "/yarn-site.xml").isFile()) {
			LOGGER.error("yarn-site.xml is miss... ");
			return false;
		}
		return true;
	}
}
