package main;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yhaitao.tohive.utils.Common;
import com.yhaitao.tohive.utils.HeapDataUtils;

public class TestHeapDataUtils {
	public static void main(String[] args) {
		Map<String, Double> groupData = new HashMap<String, Double>();
		groupData.put("1", 5.0);
		groupData.put("2", 8.0);
		groupData.put("3", 33.0);
		groupData.put("4", 40.0);
		groupData.put("5", 14.0);
		groupData.put("6", 22.0);
		
		Map<String, Double> sortByValue = HeapDataUtils.sortByValue(groupData);
		System.err.println(Common.GSON.toJson(sortByValue));
		List<Map<String, Double>> heapData = HeapDataUtils.heapData(sortByValue, 4);
		System.err.println(Common.GSON.toJson(heapData));
//		for(Map<String, Integer> heapD : heapData) {
//			System.err.println(Common.GSON.toJson(heapD));
//		}
	}
}
