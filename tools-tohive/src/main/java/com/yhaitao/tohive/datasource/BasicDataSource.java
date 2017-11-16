package com.yhaitao.tohive.datasource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 提供基础数据源链接操作。
 * @author yhaitao
 */
public class BasicDataSource {
	/**
	 * 获取数据库链接。
	 * @param driver 数据库驱动。 例如： com.mysql.jdbc.Driver
	 * @param url 数据库链接。 例如： jdbc:mysql://172.19.10.11:3306/words?characterEncoding=utf-8
	 * @param name 数据库用户名
	 * @param password 数据库密码
	 * @return 数据库链接
	 * @throws ClassNotFoundException JDBC驱动类解析失败
	 * @throws SQLException JDBC链接创建失败
	 */
	public static Connection getConnection(String driver, String url, String name,
			String password) throws Exception {
		// 驱动类设置
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			throw new Exception("ClassNotFoundException driver : " + driver);
		}
		
		// 创建数据库链接
		try {
			Connection connection = DriverManager.getConnection(url, name, password);
			return connection;
		} catch (SQLException e) {
			throw new Exception("SQLException url : " + (url + "/" + name + "/" + password));
		}
	}
	
	/**
	 * 关闭数据库链接
	 * @param connection 链接
	 * @param resultSet 查询数据集
	 * @throws SQLException 
	 */
	public static void close(Connection connection, ResultSet resultSet) throws SQLException {
		// 关闭链接
		if(connection != null && !connection.isClosed()) {
			connection.close();
		}
		
		// 关闭数据集合
		if(resultSet != null && !resultSet.isClosed()) {
			resultSet.close();
		}
	}
	
	/**
	 * 关闭数据库链接
	 * @param connection 链接
	 * @throws SQLException 
	 */
	public static void close(Connection connection) throws SQLException {
		// 关闭链接
		if(connection != null && !connection.isClosed()) {
			connection.close();
		}
	}
	
	/**
	 * 
	 * @param statement
	 * @throws SQLException 
	 */
	public static void close(Statement statement) throws SQLException {
		if(statement != null && !statement.isClosed()) {
			statement.close();
		}
	}
	
	/**
	 * 关闭数据集
	 * @param resultSet 数据集
	 * @throws SQLException 
	 */
	public static void close(ResultSet resultSet) throws SQLException {
		// 关闭链接
		if(resultSet != null && !resultSet.isClosed()) {
			resultSet.close();
		}
	}
}
