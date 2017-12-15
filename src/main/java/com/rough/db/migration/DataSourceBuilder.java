/**
 * @Copyright:2016-2020 www.fixuan.com Inc. All rights reserved.
 */
package com.rough.db.migration;

import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

public class DataSourceBuilder
{
	public static final String TYPE_SOURCE = "source";

	public static final String TYPE_TARGET = "target";

	private static final String JDBC_DRIVER = "jdbc.%s.driver";

	private static final String JDBC_URL = "jdbc.%s.url";

	private static final String JDBC_USERNAME = "jdbc.%s.username";

	private static final String JDBC_PASSWORD = "jdbc.%s.password";

	private Properties properties;

	public DataSourceBuilder()
	{
		// 加载配置文件
		properties = new Properties();

		try
		{
			properties.load(Thread.currentThread().getContextClassLoader()
			                .getResourceAsStream("dbcp.properties"));
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public DataSource buildDataSource(String type)
	{

		BasicDataSource dataSource = setupDataSource(driver(type),
		                username(type), password(type), url(type));

		// 数据库连接配置，主要由数据库驱动提供

		return dataSource;
	}

	private BasicDataSource setupDataSource(String driver, String username,
	                String password, String url)
	{
		BasicDataSource dataSource = new BasicDataSource();

		// 基础配置
		dataSource.setUsername(username);
		dataSource.setPassword(password);
		dataSource.setUrl(url);
		dataSource.setDriverClassName(driver);

		return dataSource;
	}

	private String driver(String type)
	{
		return properties.getProperty(String.format(JDBC_DRIVER, type));
	}

	private String url(String type)
	{
		return properties.getProperty(String.format(JDBC_URL, type));
	}

	private String username(String type)
	{
		return properties.getProperty(String.format(JDBC_USERNAME, type));
	}

	private String password(String type)
	{
		return properties.getProperty(String.format(JDBC_PASSWORD, type));
	}

}
