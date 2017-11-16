package com.yhaitao.tohive.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 数据分堆
 * @author yhaitao
 */
public class HeapDataUtils {
	/**
	 * 数据均分。尽可能将数据均分，返回成指定数据堆。
	 * @param groupData 原始数据。 key为数据类型，value为数据数量。
	 * @param headNum 最终数据堆数
	 * @return 堆数据列表
	 */
	public static List<Map<String, Double>> heapData(Map<String, Double> groupData, int headNum) {
		// 当分堆数量小于已有键数量时
		List<Map<String, Double>> heapList = new ArrayList<Map<String, Double>>();
		if(groupData.size() <= headNum) {
			Iterator<Entry<String, Double>> iterator = groupData.entrySet().iterator();
			while(iterator.hasNext()) {
				Entry<String, Double> next = iterator.next();
				Map<String, Double> oneDataMap = new HashMap<String, Double>();
				oneDataMap.put(next.getKey(), next.getValue());
				heapList.add(oneDataMap);
			}
			return heapList;
		}
		
		// 数据均分
		Iterator<Entry<String, Double>> iterator = groupData.entrySet().iterator();
		while(iterator.hasNext()) {
			Entry<String, Double> next = iterator.next();
			String key = next.getKey();
			Double value = next.getValue();
			
			// 先填充满需要的堆数量
			if(heapList.size() < headNum) {
				Map<String, Double> oneDataMap = new HashMap<String, Double>();
				oneDataMap.put(key, value);
				heapList.add(oneDataMap);
			} else {
				// 填充满堆数量后，将数据填充到最小的堆中。
				insertIntoMinHeap(heapList, key, value);
			}
		}
		return heapList;
	}
	
	/**
	 * 将数据填充到最小的堆中。
	 * @param heapList 堆列表
	 * @param key 数据类别
	 * @param value 数据类别的数据量
	 */
	private static void insertIntoMinHeap(List<Map<String, Double>> heapList,
			String key, Double value) {
		// 找出最小堆的位置
		int size = heapList.size();
		double minCount = Integer.MAX_VALUE;
		int minIndex = 0;
		for(int i = 0; i < size; i++) {
			double countMapValue = countMapValue(heapList.get(i));
			if(countMapValue < minCount) {
				minCount = countMapValue;
				minIndex = i;
			}
		}
		// 数据添加到最小的数据堆中
		Map<String, Double> map = heapList.get(minIndex);
		map.put(key, value);
		heapList.set(minIndex, map);
	}
	
	/**
	 * 计算某堆数据总量。
	 * @param heap 数据堆
	 * @return 数据堆数据总和
	 */
	public static double countMapValue(Map<String, Double> heap) {
		double total = 0;
		Iterator<Double> iterator = heap.values().iterator();
		while(iterator.hasNext()) {
			Double next = Double.valueOf(iterator.next());
			total = next.doubleValue() + total;
		}
		return total;
	}
	
	/**
	 * Map对象根据Value进行排序
	 * @param map
	 * @return
	 */
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
			Map<K, V> map) {
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}
}
