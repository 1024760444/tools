package main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yhaitao.tohive.ToHiveInputSplit;
import com.yhaitao.tohive.datasource.BasicDataSource;
import com.yhaitao.tohive.utils.Common;

public class TestQueryGroupBy {
	/**
	 * 日志对象
	 */
	private final static Logger LOGGER = LoggerFactory.getLogger(ToHiveInputSplit.class);
	
	public static void main(String[] args) throws Exception {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://192.168.124.130:3306/chinaz?characterEncoding=utf-8";
		String name = "root";
		String password = "123456";
		// 创建数据库链接
		Connection connection = BasicDataSource.getConnection(driver, url, name, password);
		Map<String, Integer> queryGroupBy = queryGroupBy(connection);
		System.err.println(Common.GSON.toJson(queryGroupBy));
	}
	
	/**
	 * 根据需要分片的字段，查询统计每个值对应的数据量。
	 * @param connection 数据库链接
	 * @param conf 配置
	 * @return 分堆基础数据
	 * @throws SQLException 
	 */
	public static Map<String, Integer> queryGroupBy(Connection connection) throws SQLException {
		// 基本参数 
		String jdbcPartitionKey = "superId";
		String tableName = "t_mark_chinaz_words";
		
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
		Map<String, Integer> groupData = new HashMap<String, Integer>();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(querySql);
		if(resultSet != null) {
			while(resultSet.next()) {
				String key = String.valueOf(resultSet.getObject(jdbcPartitionKey));
				int value = resultSet.getInt("countValue");
				if(StringUtils.isNotBlank(key)) {
					groupData.put(key, value);
				}
			}
			BasicDataSource.close(resultSet);
		}
		return groupData;
	}
}
