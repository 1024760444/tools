package com.yhaitao.tohive.mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.yhaitao.tohive.utils.Common;

/**
 * 
 * @author yhaitao
 *
 */
public class ToHiveMapper extends Mapper<Text, Text, NullWritable, Text> {
	private MultipleOutputs<NullWritable, Text> mOutputs;
	private String temp_path;
	private String hive_p_key;
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.temp_path = conf.get(FileOutputFormat.OUTDIR);
		this.hive_p_key = conf.get(Common.KEY_HIVE_PARTITION_KEY);
		this.mOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String baseOutputPath = new StringBuffer()
			.append(this.temp_path)
			.append("/")
			.append(this.hive_p_key)
			.append("=")
			.append(key.toString())
			.append("/")
			.append(key.toString()).toString();
		this.mOutputs.write(NullWritable.get(), value, baseOutputPath);
	}

	public void cleanup(Context context) throws IOException,InterruptedException {
		this.mOutputs.close();
	}
}
