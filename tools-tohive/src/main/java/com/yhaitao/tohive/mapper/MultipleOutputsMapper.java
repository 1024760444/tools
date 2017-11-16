package com.yhaitao.tohive.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 多目录输出Map任务。 
 * 该Map任务不做任务处理， 只负责数据传递；通过Reduce集中处理任务。
 * @author yhaitao
 *
 */
public class MultipleOutputsMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}
