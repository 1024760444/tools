package com.yhaitao.tohive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.tohive.datasource.BasicDataSource;
import com.yhaitao.tohive.utils.Common;
import com.yhaitao.tohive.utils.HeapDataUtils;

/**
 * 数据关系型数据库提交到Hive数据库中。
 * @author yhaitao
 *
 */
public class ToHiveInputFormat extends InputFormat<Text, Text> {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(ToHiveInputFormat.class);
	
	/**
	 * 数据分片
	 */
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String driver = conf.get(Common.KEY_JDBC_DRIVER);
		String url = conf.get(Common.KEY_JDBC_URL);
		String name = conf.get(Common.KEY_JDBC_NAME);
		String password = conf.get(Common.KEY_JDBC_PASSWORD);
		int headNum = conf.getInt(Common.KEY_JDBC_MAP_NUMBER, 4);
		conf.setInt("mapreduce.job.max.split.locations", headNum);
		
		// 数据分片
		List<InputSplit> splitList = new ArrayList<InputSplit>();
		Connection connection = null;
		try {
			// 创建数据库链接
			connection = BasicDataSource.getConnection(driver, url, name, password);
			// 查询分堆基础数据
			Map<String, Double> groupData = queryGroupBy(connection, conf);
			LOGGER.info("getSplits groupData : {}. ", Common.GSON.toJson(groupData));
			// 创建分区
			addPartition2Hive(conf, groupData);
			// 键分堆
			List<Map<String, Double>> heapList = HeapDataUtils.heapData(groupData, headNum);
			LOGGER.info("getSplits heapList : {}. ", Common.GSON.toJson(heapList));
			// 创建数据分片
			for(Map<String, Double> heapData : heapList) {
				splitList.add(new ToHiveInputSplit(heapData));
			}
		} catch (Exception e) {
			LOGGER.error("getSplits driver : {}, url : {}, name : {}, password : {}, Exception : {}. ", 
					driver, url, name, password, e.getMessage());
		}
		return splitList;
	}
	
	/**
	 * 往Hive创建分区
	 * @param conf 配置
	 * @param groupData 分区值
	 */
	private void addPartition2Hive(Configuration conf, Map<String, Double> groupData) {
		// TODO Auto-generated method stub
		// Hive链接配置
		String driver = conf.get(Common.KEY_HIVE_DRIVER);
		String url = conf.get(Common.KEY_HIVE_URL);
		String name = conf.get(Common.KEY_HIVE_NAME);
		String password = conf.get(Common.KEY_HIVE_PASSWORD);
		String hiveTable = conf.get(Common.KEY_HIVE_TABLE_NAME);
		String hive_p_key = conf.get(Common.KEY_HIVE_PARTITION_KEY);
		// 创建数据库链接
		try {
			Connection hiveConn = BasicDataSource.getConnection(driver, url, name, password);
			Iterator<String> iterator = groupData.keySet().iterator();
			while(iterator.hasNext()) {
				// 组建SQL语句
				String keyValue = iterator.next();
				String sql = new StringBuffer()
					.append("alter table ")
					.append(hiveTable)
					.append(" add partition (")
					.append(hive_p_key)
					.append("='")
					.append(keyValue)
					.append("')").toString();
				
				// 执行创建分区
				Statement statement = hiveConn.createStatement();
				statement.execute(sql);
				BasicDataSource.close(statement);
			}
			BasicDataSource.close(hiveConn);
		} catch (Exception e) {
			LOGGER.error("initialize driver : {}, url : {}, name : {}, password : {}, Exception : {}. ", 
					driver, url, name, password, e.getMessage());
		}
	}
	
	/**
	 * 创建数据迭代器。
	 */
	@Override
	public RecordReader<Text, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new ToHiveRecordReader();
	}
	
	/**
	 * 根据需要分片的字段，查询统计每个值对应的数据量。
	 * @param connection 数据库链接
	 * @param conf 配置
	 * @return 分堆基础数据
	 * @throws SQLException 
	 */
	private Map<String, Double> queryGroupBy(Connection connection,
			Configuration conf) throws SQLException {
		// 基本参数 
		String jdbcPartitionKey = conf.get(Common.KEY_JDBC_PARTITION_KEY);
		String tableName = conf.get(Common.KEY_JDBC_TABLE_NAME);
		
		/**
		 * 组建SQL语句： 
		 * select superId, count(*) as countValue from t_mark_chinaz_words group by superId;
		 */
		String querySql = new StringBuffer()
			.append("select ")
			.append(jdbcPartitionKey)
			.append(", count(*) as countValue ")
			.append(" from ")
			.append(tableName)
			.append(" group by ")
			.append(jdbcPartitionKey)
			.toString();
		LOGGER.info("queryGroupBy querySql : {}. ", querySql);
		
		// 数据查询
		Map<String, Double> groupData = new HashMap<String, Double>();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(querySql);
		if(resultSet != null) {
			while(resultSet.next()) {
				String key = String.valueOf(resultSet.getObject(jdbcPartitionKey));
				int value = resultSet.getInt("countValue");
				if(StringUtils.isNotBlank(key)) {
					groupData.put(key, Double.valueOf(value));
				}
			}
			BasicDataSource.close(resultSet);
		}
		return groupData;
	}
}
