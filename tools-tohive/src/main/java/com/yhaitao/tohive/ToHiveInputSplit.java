package com.yhaitao.tohive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.tohive.utils.Common;
import com.yhaitao.tohive.utils.HeapDataUtils;

/**
 * 数据输入分片。
 * @author yhaitao
 *
 */
public class ToHiveInputSplit extends InputSplit implements Writable  {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(ToHiveInputSplit.class);
	
	/**
	 * 堆数据。
	 */
	private Map<String, Double> heapData;
	
	
	public ToHiveInputSplit() {}
	
	/**
	 * 一个数据堆，定义一个数据分片。
	 * @param heapData 堆数据
	 */
	public ToHiveInputSplit(Map<String, Double> heapData) {
		this.heapData = heapData;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		double countMapValue = HeapDataUtils.countMapValue(heapData);
		LOGGER.info("getLength return countMapValue : {}. ", countMapValue);
		return Double.valueOf(countMapValue).longValue();
	}
	
	/**
	 * 数据的位置，即是数据的键值。
	 */
	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		if(this.heapData == null || this.heapData.isEmpty()) {
			LOGGER.info("getLocations return null... ");
			return new String[] {};
		} else {
			int size = this.heapData.size();
			String[] locations = new String[size];
			Iterator<String> iterator = this.heapData.keySet().iterator();
			int index = 0;
			while(iterator.hasNext()) {
				String next = iterator.next();
				locations[index] = next;
				index++;
			}
			LOGGER.info("getLocations return locations : {}. ", Common.GSON.toJson(locations));
			return locations;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, Common.GSON.toJson(heapData));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		// Text.readString(in);
		this.heapData = Common.GSON.fromJson(Text.readString(in), Map.class);
	}
}
