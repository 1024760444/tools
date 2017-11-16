package main;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.yhaitao.tohive.datasource.BasicDataSource;

public class Test {
	public static void main(String[] args) throws Exception {
		// Hive链接配置
		String driver = "org.apache.hive.jdbc.HiveDriver";
		String url = "jdbc:hive2://192.168.124.128:10000/ichart";
		String name = "root";
		String password = "123456";
		// 创建数据库链接
		Connection hiveConn = BasicDataSource.getConnection(driver, url, name, password);
		
		// alter table t_external_words add partition (psuperid='23')
		String sql = new StringBuffer()
				.append("alter table ")
				.append("t_external_words")
				.append(" add partition (")
				.append("psuperid")
				.append("='")
				.append(23)
				.append("')").toString();
		
		// 执行创建分区
		boolean addPResult = false;
		try {
			Statement statement = hiveConn.createStatement();
			addPResult = statement.execute(sql);
			BasicDataSource.close(statement);
			System.err.println(addPResult);
		} catch (SQLException e) {
			throw new IOException("Reduce cleanup SQLException : " + e.getMessage());
		}
	}
}
