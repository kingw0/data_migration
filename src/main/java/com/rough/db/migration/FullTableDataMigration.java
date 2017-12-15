/**
 * @Copyright:2016-2020 www.fixuan.com Inc. All rights reserved.
 */
package com.rough.db.migration;

import java.sql.Blob;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullTableDataMigration
{
	/**
	 * 
	 */
	private static final Logger logger = LoggerFactory
	                .getLogger(FullTableDataMigration.class);

	/**
	 * 
	 * @param originDataSource
	 * @param targetDataSource
	 * @throws SQLException
	 */
	public void migration(DataSource originDataSource,
	                DataSource targetDataSource) throws SQLException
	{
		QueryRunner originRunner = new QueryRunner(originDataSource);

		QueryRunner targetRunner = new QueryRunner(targetDataSource);

		Set<String> originTables = fetchTables(originRunner,
		                fetchOracleTablesSqlStatement());

		Set<String> targetTables = fetchTables(targetRunner,
		                fetchMySQLTablesSqlStatement());

		if (CollectionUtils.isNotEmpty(originTables)
		                && CollectionUtils.isNotEmpty(targetTables))
		{
			Set<String> columns = null;

			OracleDataIterator iter = null;

			List<Object[]> results = null;

			targetRunner.execute("SET FOREIGN_KEY_CHECKS=0");

			int totalTable = 0;

			try
			{

				for (String originTable : originTables)
				{
					String targetTable = convertTable(originTable);

					if (!isTargetTableExists(targetTable, targetTables))
					{
						logger.error("{} does not exist in target database!",
						                targetTable);

						continue;
					}

					// 清空目标表数据
					targetRunner.execute("truncate table " + targetTable);

					logger.info("Begin migrate table {} to {}!", originTable,
					                targetTable);

					columns = fetchColumns(originRunner,
					                fetchOracleColumnsSqlStatement(),
					                originTable);

					iter = new OracleDataIterator(originRunner,
					                fetchOracleDataSqlStatement(originTable,
					                                columns));

					int total = 0;

					while (iter.hasNext())
					{
						results = iter.next();

						// 批量插入
						Object[][] params = new Object[results.size()][];

						for (int i = 0; i < results.size(); i++)
						{
							params[i] = new Object[results.get(i).length];

							for (int j = 0; j < results.get(i).length; j++)
							{
								params[i][j] = results.get(i)[j];
							}
						}

						targetRunner.batch(fetchInsertDataSqlStatement(
						                targetTable, columns), params);

						logger.info("Insert {} data into table {}!",
						                results.size(), targetTable);

						total += results.size();
					}

					logger.info("Finish migrate table {} to {},the number of migrated data is {}!",
					                originTable, targetTable, total);

					totalTable++;
				}

				logger.info("Migrate total {} tables!", totalTable);
			}
			finally
			{
				targetRunner.execute("SET FOREIGN_KEY_CHECKS=1");
			}
		}
	}

	private String convertTable(String originTable)
	{
		if (!originTable.startsWith("ACT_"))
		{
			return originTable.toLowerCase();
		}
		else
		{
			return originTable;
		}
	}

	private boolean isTargetTableExists(String table, Set<String> targetTables)
	{
		return targetTables.contains(table);
	}

	/**
	 * 查询
	 * 
	 * @param runner
	 * @param fetchTablesSql
	 * @return
	 * @throws SQLException
	 */
	private Set<String> fetchTables(QueryRunner runner, String fetchTablesSql)
	                throws SQLException
	{
		return runner.query(fetchTablesSql, new ResultSetHandler<Set<String>>()
		{

			@Override
			public Set<String> handle(ResultSet rs) throws SQLException
			{
				Set<String> tables = new LinkedHashSet<>();

				while (rs.next())
				{
					tables.add(rs.getString(1));
				}

				return tables;
			}

		});
	}

	private Set<String> fetchColumns(QueryRunner runner, String fetchColumnsSql,
	                String table) throws SQLException
	{
		return runner.query(fetchColumnsSql, new ResultSetHandler<Set<String>>()
		{

			@Override
			public Set<String> handle(ResultSet rs) throws SQLException
			{
				Set<String> tables = new LinkedHashSet<>();

				while (rs.next())
				{
					tables.add(rs.getString(1));
				}

				return tables;
			}

		}, table);
	}

	private String fetchOracleTablesSqlStatement()
	{
		return "select TABLE_NAME from USER_TABLES";
	}

	private String fetchOracleColumnsSqlStatement()
	{
		return "select COLUMN_NAME from USER_TAB_COLUMNS where TABLE_NAME = ?";
	}

	private String fetchOracleDataSqlStatement(String table,
	                Set<String> columns)
	{
		StringBuilder sqlBuilder = new StringBuilder("select ");

		for (String column : columns)
		{
			sqlBuilder.append("\"").append(column).append("\", ");
		}

		int length = sqlBuilder.length();

		sqlBuilder.delete(length - 2, length);

		sqlBuilder.append(" from ( select t.*, rownum rn from ").append(table)
		                .append(" t where rownum <= ?) where rn >= ?");

		return sqlBuilder.toString();
	}

	private String fetchMySQLTablesSqlStatement()
	{
		return "select TABLE_NAME from information_schema.TABLES where table_type ='BASE TABLE'";
	}

	public String fetchInsertDataSqlStatement(String table, Set<String> columns)
	{
		StringBuilder sqlBuilder = new StringBuilder("insert into ")
		                .append(table).append(" (");

		for (String column : columns)
		{
			sqlBuilder.append("`").append(column).append("`, ");
		}

		int length = sqlBuilder.length();

		sqlBuilder.delete(length - 2, length);

		sqlBuilder.append(") values (");

		for (int i = 0; i < columns.size(); i++)
		{
			sqlBuilder.append("?, ");
		}

		length = sqlBuilder.length();

		sqlBuilder.delete(length - 2, length);

		sqlBuilder.append(")");

		return sqlBuilder.toString();
	}

	private static Object getObject(ResultSet rs, ResultSetMetaData meta,
	                int columnIndex) throws SQLException
	{
		JDBCType type = JDBCType.valueOf(meta.getColumnType(columnIndex));

		Object result = null;

		switch (type)
		{
			case NULL:
				break;
			case NUMERIC:
				result = rs.getLong(columnIndex);
				break;
			case INTEGER:
				result = rs.getInt(columnIndex);
				break;
			case VARCHAR:
			case CHAR:
			case NVARCHAR:
			case NCHAR:
				result = rs.getString(columnIndex);
				break;
			case DATE:
			case TIMESTAMP:
				result = rs.getDate(columnIndex);
				break;
			case BLOB:
				Blob blob = rs.getBlob(columnIndex);
				result = blob == null ? null
				                : blob.getBytes(1, (int) blob.length());
				break;
			default:
				break;
		}

		return result;
	}

	/**
	 * 
	 * @author Teddy Tang,kingw0@126.com
	 *
	 */
	public static final class OracleDataIterator
	                implements Iterator<List<Object[]>>
	{
		private static final int DEFAULT_PAGE_SIZE = 200;

		private QueryRunner runner;

		private String sql;

		private int start = 1;

		private int end = DEFAULT_PAGE_SIZE;

		private List<Object[]> results = null;

		/**
		 * 
		 * @param runner
		 */
		public OracleDataIterator(QueryRunner runner, String sql)
		{
			this.runner = runner;

			this.sql = sql;
		}

		@Override
		public boolean hasNext()
		{
			try
			{
				results = runner.query(sql,
				                new ResultSetHandler<List<Object[]>>()
				                {

					                @Override
					                public List<Object[]> handle(ResultSet rs)
					                                throws SQLException
					                {
						                if (!rs.next())
						                {
							                return null;
						                }

						                List<Object[]> resultList = new ArrayList<>();

						                ResultSetMetaData meta = rs
						                                .getMetaData();

						                int cols = meta.getColumnCount();

						                Object[] result = null;

						                do
						                {
							                result = new Object[cols];

							                for (int i = 0; i < cols; i++)
							                {
								                int columnIndex = i + 1;

								                result[i] = getObject(rs, meta,
								                                columnIndex);
							                }

							                resultList.add(result);
						                } while (rs.next());

						                return resultList;
					                }

				                }, end, start);
			}
			catch (SQLException e)
			{
				logger.error("Failed to excute sql[{}] with params[{},{}] ",
				                sql, end, start);

				throw new RuntimeException(e);
			}

			return results != null;
		}

		@Override
		public List<Object[]> next()
		{
			start += DEFAULT_PAGE_SIZE;

			end += DEFAULT_PAGE_SIZE;

			return results;
		}

	}

}
