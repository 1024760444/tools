package com.yhaitao.tohive.utils;

import com.google.gson.Gson;

/**
 * 通用工具定义。
 * @author yhaitao
 * 
 */
public class Common {
	/**
	 * GSON数据格式。
	 */
	public static final Gson GSON = new Gson();
	
	/**
	 * 从mysql一次性读取数据数量
	 */
	public static final long DEFAULT_PAGESIZE = 1000;
	
	/**
	 * 数据默认分隔符
	 */
	public static final char DEFAULT_SEPARATOR = '\u0001';
	
	/**
	 * 数据库驱动
	 */
	public static final String KEY_JDBC_DRIVER = "jdbc.driver";
	
	/**
	 * 数据库URL
	 */
	public static final String KEY_JDBC_URL = "jdbc.url";
	
	/**
	 * 数据库名称
	 */
	public static final String KEY_JDBC_NAME = "jdbc.name";
	
	/**
	 * 数据库密码
	 */
	public static final String KEY_JDBC_PASSWORD = "jdbc.password";

	/**
	 * 需要导出数据的表名
	 */
	public static final String KEY_JDBC_TABLE_NAME = "jdbc.table.name";
	
	/**
	 * 需要导出数据表中分区的列名
	 */
	public static final String KEY_JDBC_PARTITION_KEY = "jdbc.partition.key";

	/**
	 * 数据库表导出数据字段列表，以","分割。
	 */
	public static final String KEY_JDBC_TABLE_COLUMNS = "jdbc.table.columns";
	
	/**
	 * 数据库导出数据分隔符
	 */
	public static final String KEY_JDBC_DATA_SEPARATOR = "jdbc.data.separator";
	
	/**
	 * 数据库每次读取数据量
	 */
	public static final String KEY_JDBC_DATA_PAGESIZE = "jdbc.data.pageSize";
	
	/**
	 * 分片数量
	 */
	public static final String KEY_JDBC_MAP_NUMBER = "jdbc.map.number";
	
	/**
	 * 临时路径。HDFS临时存储路径。
	 */
	public static final String KEY_HIVE_DST_TMP_PATH = "hive.dst.tmp.path";
	
	/**
	 * Hive表分区字段名。
	 */
	public static final String KEY_HIVE_PARTITION_KEY = "hive.partition.key";
	
	/**
	 * 保存的文件的最大长度
	 */
	public static final String KEY_HIVE_FILE_SIZE = "hive.file.size";
	
	/**
	 * 换行
	 */
	public static final char KEY_WRAP = '\n';
	
	/**
	 * Hive驱动关键字
	 */
	public static final String KEY_HIVE_DRIVER = "hive.driver";
	
	/**
	 * Hive链接地址关键字
	 */
	public static final String KEY_HIVE_URL = "hive.url";
	
	/**
	 * Hive链接用户名
	 */
	public static final String KEY_HIVE_NAME = "hive.name";
	
	/**
	 * Hive链接用户密码
	 */
	public static final String KEY_HIVE_PASSWORD = "hive.password";
	
	/**
	 * Hive中，数据导入表名的关键字
	 */
	public static final String KEY_HIVE_TABLE_NAME = "hive.table.name";
	
	/**
	 * 需要额外引入的jar包路径，jar上传到hdfs上。
	 */
	public static final String KEY_HDFS_PATH_EXT_JARS = "hdfs.path.ext.jars";
}
