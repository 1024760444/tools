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
public class TestLinkHive {
	public static void main(String[] args) {
		String password = "123456";
		String name = "root";
		String url = "jdbc:hive2://hdp1:10000/ichart";
		String driver = "org.apache.hive.jdbc.HiveDriver";
		String querySql = "select * from ichart_user_count where pdate='20170808'";
		
		try {
			/**
			 * Class.forName(driver);
			 * Connection connection = DriverManager.getConnection(url, name, password);
			 */
			Connection connection = BasicDataSource.getConnection(driver, url, name, password);
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(querySql);
			while (resultSet.next()) {
                System.out.println(resultSet.getString("cdate") 
                		+ " " + resultSet.getString("http_host")
                		+ " " + resultSet.getString("localipaddress")
                		+ " " + resultSet.getInt("ip_count")
                		+ " " + resultSet.getInt("pw_count"));
            }
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
