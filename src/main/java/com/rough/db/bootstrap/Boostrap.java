/**
 * @Copyright:2016-2020 www.fixuan.com Inc. All rights reserved.
 */
package com.rough.db.bootstrap;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rough.db.migration.DataSourceBuilder;
import com.rough.db.migration.FullTableDataMigration;

/**
 * 
 * @author Teddy Tang,kingw0@126.com
 *
 */
public class Boostrap
{
	private static final Logger logger = LoggerFactory
	                .getLogger(Boostrap.class);

	public static void main(String[] args)
	{
		logger.info("===Begin migration===");

		FullTableDataMigration migration = new FullTableDataMigration();

		DataSourceBuilder builder = new DataSourceBuilder();

		try
		{
			migration.migration(
			                builder.buildDataSource(
			                                DataSourceBuilder.TYPE_SOURCE),
			                builder.buildDataSource(
			                                DataSourceBuilder.TYPE_TARGET));
		}
		catch (SQLException e)
		{
			logger.info("===Migration failed!===", e);
		}

		logger.info("===Migration success!===");
	}
}
