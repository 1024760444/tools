package com.yhaitao.tohive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.tohive.datasource.BasicDataSource;
import com.yhaitao.tohive.utils.Common;

/**
 * Mysql数据迭代器。
 * @author yhaitao
 *
 */
public class ToHiveRecordReader extends RecordReader<Text, Text> {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(ToHiveRecordReader.class);
	
	/**
	 * 数据数据链接。
	 */
	private Connection connection;
	
	/**
	 * 堆中分类的值。
	 */
	private String[] locations;
	
	/**
	 * 对数据序号。
	 */
	private int locationsIndex = 0;
	
	/**
	 * 数据分割符。
	 */
	private char dataSeparator = Common.DEFAULT_SEPARATOR;
	
	/**
	 * 需要查询的表的名称。
	 */
	private String tableName;
	
	/**
	 * 需要分区的字段。
	 */
	private String jdbcPartitionKey;
	
	/**
	 * 数据查询列表，以逗号分割。
	 */
	private String queryString;
	
	/**
	 * 数据查询列表，分割后字段的个数。
	 */
	private int queryStringSize;
	
	/**
	 * 每次数据查询量。
	 */
	private long queryDataSize = Common.DEFAULT_PAGESIZE;
	
	/**
	 * 当前数据的键。
	 */
	private String currentKey;
	
	/**
	 * 当前数据的值。
	 */
	private String currentValue;
	
	/**
	 * 查询到内存中的数据迭代器
	 */
	private Iterator<String> iterator;
	
	/**
	 * 当前分片数据量。
	 */
	private long currentCount = 0;
	
	/**
	 * 数据分片已经处理数据量。计算总进度的分子。
	 */
	private long splitDataCount = 0;
	
	/**
	 * 当前分片总数据量。计算总进度的分母。
	 */
	private long splitDataSize = 0;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// 基本配置获取
		Configuration conf = context.getConfiguration();
		String driver = conf.get(Common.KEY_JDBC_DRIVER);
		String url = conf.get(Common.KEY_JDBC_URL);
		String name = conf.get(Common.KEY_JDBC_NAME);
		String password = conf.get(Common.KEY_JDBC_PASSWORD);
		
		// 堆中分类的数据列表
		this.locations = split.getLocations();
		this.splitDataSize = split.getLength();
		// 分隔符设置
		this.dataSeparator = (char) conf.getInt(Common.KEY_JDBC_DATA_SEPARATOR, 1);
		// Mysql数据查询列表
		this.queryString = conf.get(Common.KEY_JDBC_TABLE_COLUMNS);
		this.queryStringSize = this.queryString.split("\\,").length;
		this.jdbcPartitionKey = conf.get(Common.KEY_JDBC_PARTITION_KEY);
		this.tableName = conf.get(Common.KEY_JDBC_TABLE_NAME);
		// 查询数量设置
		this.queryDataSize = conf.getLong(Common.KEY_JDBC_DATA_PAGESIZE, Common.DEFAULT_PAGESIZE);
		
		// 创建数据库链接
		try {
			this.connection = BasicDataSource.getConnection(driver, url, name, password);
		} catch (Exception e) {
			LOGGER.error("initialize driver : {}, url : {}, name : {}, password : {}, Exception : {}. ", 
					driver, url, name, password, e.getMessage());
			throw new InterruptedException("initialize driver : " + driver 
					+ ", url : " + url 
					+ ", name : " + name 
					+ ", password : " + password + ", " + e.getMessage());
		}
		
		// 初始化数据迭代器
		this.locationsIndex = 0;
		this.currentKey = this.locations[this.locationsIndex];
		this.iterator = initIterator();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// 当前迭代数据
		boolean hasNext = this.iterator.hasNext();
		if(!hasNext) {
			// 使用下一批迭代数据
			this.iterator = initIterator();
			hasNext = this.iterator.hasNext();
			while(!hasNext && (this.locationsIndex + 1 < this.locations.length)) {
				this.locationsIndex++;
				this.currentCount = 0;
				this.currentKey = this.locations[this.locationsIndex];
				// 使用下一批迭代数据
				this.iterator = initIterator();
				hasNext = this.iterator.hasNext();
			}
		}
		
		// 存在当前值
		if(hasNext) {
			this.currentValue = this.iterator.next();
			this.currentCount++;
			this.splitDataCount++;
		} else {
			try {
				BasicDataSource.close(connection);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return hasNext;
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		return new Text(this.currentKey);
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(this.currentValue);
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return Float.valueOf(this.splitDataCount/this.splitDataSize);
	}

	@Override
	public void close() throws IOException {
		try {
			BasicDataSource.close(this.connection);
		} catch (SQLException e) {
			LOGGER.error("close SQLException : {}. ", e.getMessage());
		}
	}
	
	/**
	 * 初始化数据迭代器
	 * @return 数据迭代器
	 */
	private Iterator<String> initIterator() {
		// 查询数据总量的SQL语句
		String sqlSb = new StringBuffer()
			.append("select ").append(this.queryString)
			.append(" from ").append(this.tableName)
			.append(" where ").append(this.jdbcPartitionKey).append(" = '").append(this.currentKey).append("'")
			.append("limit ").append(this.currentCount).append(", ").append(this.queryDataSize).toString();
		LOGGER.info("initIterator sqlSb : {}. ", sqlSb);
		
		// 查询数据列表
		List<String> dataList = new ArrayList<String>();
		
		// 查询键对应的所有value值
		try {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sqlSb);
			if(resultSet != null) {
				while(resultSet.next()) {
					StringBuffer sb = new StringBuffer();
					for(int i = 1; i <= this.queryStringSize; i++) {
						String string = String.valueOf(resultSet.getObject(i));
						sb.append(string);
						if(i != this.queryStringSize) {
							sb.append(this.dataSeparator);
						}
					}
					dataList.add(sb.toString());
				}
			}
			
			// 关闭Mysql链接
			BasicDataSource.close(resultSet);
		} catch (SQLException e) {
			LOGGER.error("initIterator SQLException : {}. ", e.getMessage());
		}
		return dataList.iterator();
	}
	
}
