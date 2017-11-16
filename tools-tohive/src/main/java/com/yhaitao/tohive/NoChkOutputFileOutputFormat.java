package com.yhaitao.tohive;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NoChkOutputFileOutputFormat extends TextOutputFormat<Text, Text> {
	/**
	 * 不对文件输出路径做任何校验
	 */
	public void checkOutputSpecs(JobContext job) 
			throws FileAlreadyExistsException, IOException {}
}
