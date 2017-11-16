package com.yhaitao.tohive.reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.yhaitao.tohive.utils.Common;

/**
 * 数据导入Reduce任务。
 * 基于多目录写数据类MultipleOutputs。
 * 通过数据的key设置hive分区，将数据保存到指定分区路径下。
 * @author yhaitao
 *
 */
public class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
	/**
	 * 多目录写数据对象。
	 */
	private MultipleOutputs<NullWritable, Text> mOutputs;
	
	/**
	 * 数据保存临时目录，通过配置： mapreduce.output.fileoutputformat.outputdir 设置。
	 */
	private String temp_path;
	
	/**
	 * HIVE表分区字段名称，通过配置 : hive.partition.key 设置。
	 */
	private String hive_p_key;
	
	/**
	 * 单个文件最大数据长度。
	 */
	private int hive_file_size;
	
	/**
	 * Reduce任务初始化。
	 */
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		// 基本配置
		this.temp_path = conf.get(FileOutputFormat.OUTDIR);
		this.hive_p_key = conf.get(Common.KEY_HIVE_PARTITION_KEY);
		this.hive_file_size = conf.getInt(Common.KEY_HIVE_FILE_SIZE, 67108864);
		this.mOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	/**
	 * Reduce任务数据处理
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 数据文件的基本路径
		StringBuffer baseOutputPath = new StringBuffer()
			.append(this.temp_path)
			.append("/")
			.append(this.hive_p_key)
			.append("=")
			.append(key.toString())
			.append("/");
		
		// 多个文件的文件序号
		int fileIndex = 0;
		// 数据队列 ： 以便批量写数据。
		StringBuffer sbDataList = new StringBuffer();
		for (Text value : values) {
			if(sbDataList.length() != 0) {
				sbDataList.append(Common.KEY_WRAP);
			}
			sbDataList.append(value.toString());
			
			// 追加数据到当前数据队列
			if(sbDataList.length() >= this.hive_file_size) {
				// 数据达到64M，写数据到hdfs
				this.mOutputs.write(NullWritable.get(), 
						new Text(sbDataList.toString()), 
						(baseOutputPath.toString() + String.valueOf(fileIndex)));
				// 清空数据，追加数据文件序号
				sbDataList.delete(0, sbDataList.length());
				fileIndex++;
			}
		}
		
		// 写最后的遗留数据
		if(sbDataList.length() > 1) {
			this.mOutputs.write(NullWritable.get(), 
					new Text(sbDataList.toString()), 
					(baseOutputPath.toString() + String.valueOf(fileIndex)));
		}
	}
	
	/**
	 * 结束reduce任务，关闭数据输出流。
	 */
	public void cleanup(Context context) throws IOException,InterruptedException {
		this.mOutputs.close();
	}
}
