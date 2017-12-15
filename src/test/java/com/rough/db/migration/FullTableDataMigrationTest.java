/**
 * @Copyright:2016-2020 www.fixuan.com Inc. All rights reserved.
 */
package com.rough.db.migration;

import java.sql.SQLException;

import org.junit.Test;

public class FullTableDataMigrationTest
{
	@Test
	public void testMigrateTable() throws SQLException
	{
		FullTableDataMigration migration = new FullTableDataMigration();

		DataSourceBuilder builder = new DataSourceBuilder();

		migration.migration(
		                builder.buildDataSource(DataSourceBuilder.TYPE_SOURCE),
		                builder.buildDataSource(DataSourceBuilder.TYPE_TARGET));
	}
}
