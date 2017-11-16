package main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.yhaitao.tohive.datasource.BasicDataSource;

/**
 * 测试链接hive
 * @author Administrator
 *
 */
public class TestLinkMysql {
	public static void main(String[] args) {
		String password = "123456";
		String name = "root";
		String url = "jdbc:mysql://localhost:3306/words";
		String driver = "com.mysql.jdbc.Driver";
		String querySql = "select count(*) as count from t_mark_chinaz_urls";
		
		try {
			/**
			 * Class.forName(driver);
			 * Connection connection = DriverManager.getConnection(url, name, password);
			 */
			Connection connection = BasicDataSource.getConnection(driver, url, name, password);
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(querySql);
			resultSet.next();
			int int1 = resultSet.getInt(1);
			System.err.println(int1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
