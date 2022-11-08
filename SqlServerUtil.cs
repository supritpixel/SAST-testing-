using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using svx.lib.constants;
using solvexia.svx.shared.lib.sharedutilities;
using SystemException = solvexia.svx.shared.lib.sharedutilities.SystemException;

namespace solvexia.svx.server.lib.utilities
{
    public class SqlServerUtil
    {
        public const string cnst_dbconnectionsettings = "DBConnectionSettings";
        public const string cnst_long_timeout = "LongCommandTimeout";
        private const string cnst_Sql_server_side_paging_template_for_managed_table = @"
SELECT *
FROM
(
SELECT RowNum = ROW_NUMBER() OVER (
ORDER BY _insertionDateTime), *
FROM (#CoreQuery#) AS x
) AS a
WHERE RowNum > (#PageSize# * (#PageNumber# - 1))
AND RowNum <= (#PageSize# * (#PageNumber# - 1)) + #PageSize#";

        /// <summary>
        /// Used to generate query that can return useful query for paging.
        /// </summary>
        public static string GetPageForQuery(string coreQuery, int pageNumber, long pageSize = 5000)
        {
            SharedTemplateHelper templateHelper = new SharedTemplateHelper(cnst_Sql_server_side_paging_template_for_managed_table);

            templateHelper.ReplaceParameter("CoreQuery", coreQuery);
            templateHelper.ReplaceParameter("PageNumber", pageNumber.ToString());
            templateHelper.ReplaceParameter("PageSize", pageSize.ToString());

            return templateHelper.ToString();
        }
        

        /// <summary>
        /// Create an empty datatable object that has exactly the schema of the underlying Sql table
        /// Assumptions:
        /// 1. sqlConnection is not null
        /// 2. sqlConnection is opened
        /// 3. sql tranction is not null and is started
        /// </summary>
        public static DataTable GetSchemaTableFromSqlDB(SqlConnection sqlConnection, string sqlTableName, SqlTransaction transaction, int TimeLimitInSecs = -1)
        {
            SharedValidations.ThrowException_IfObjectReferenceIsNull("sqlConnection", sqlConnection);
            SharedValidations.ThrowException_IfObjectReferenceIsNull("transaction", transaction);

            DataTable dataContainer = new DataTable();
            string sql_statement = "SELECT * FROM " + sqlTableName + " WHERE 1 = 2";
            if(TimeLimitInSecs < 0) TimeLimitInSecs = RuntimeSettings.Get(cnst_dbconnectionsettings, cnst_long_timeout, 300);

            using (SqlCommand cmd = new SqlCommand(sql_statement, sqlConnection))
            {
                cmd.Transaction = transaction;
                cmd.CommandTimeout = TimeLimitInSecs;
                using (SqlDataAdapter sqlAdapter = new SqlDataAdapter(cmd))
                {
                    if (sqlAdapter.SelectCommand != null) sqlAdapter.SelectCommand.CommandTimeout = TimeLimitInSecs;

                    sqlAdapter.FillSchema(dataContainer, SchemaType.Mapped);
                    sqlAdapter.Fill(dataContainer);
                }
            }

            return dataContainer;
        }

        /// <summary>
        /// Used to delete old data in given table
        /// Assumptions:
        /// 1. sqlConnection is not null
        /// 2. sqlConnection is opened
        /// 3. sql tranction is not null and is started
        /// </summary>
        public static void ClearTable(SqlConnection sqlConnection, string sqlTableName, SqlTransaction transaction, int TimeLimitInSecs = -1)
        {
            SharedValidations.ThrowException_IfObjectReferenceIsNull("sqlConnection", sqlConnection);
            SharedValidations.ThrowException_IfObjectReferenceIsNull("transaction", transaction);

            if (TimeLimitInSecs < 0) TimeLimitInSecs = RuntimeSettings.Get(cnst_dbconnectionsettings, cnst_long_timeout, 300);
            string sql_statement = "DELETE FROM " + sqlTableName;

            using (SqlCommand cmd = new SqlCommand(sql_statement, sqlConnection))
            {
                cmd.Transaction = transaction;
                cmd.CommandTimeout = TimeLimitInSecs;
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Used to append data from DataTable to the CDT table using SqlBulkCopy library
        /// This method may throw exception, so the caller has to handle the exception
        /// </summary>
        /// <param name="clientSession">The client session for which this method has been invoked</param>
        /// <param name="tableWithData">DataTable with data in it</param>
        /// <param name="targetTableXianID">CDT xian id</param>
        /// <param name="TimeLimitInSecs">Set the time out of appending data to sql table</param>
        public static void AppendDataToTable(Session clientSession, DataTable tableWithData, int targetTableXianID, int TimeLimitInSecs)
        {
            string connectionString = clientSession.ConnectionString;
            string tableName = ClientDefinedTable.GetSystemTableFromXianID(targetTableXianID);

            using (SqlConnection sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();

                #region Add system columns if table does not have, supplied DataTable is guaranted to have system columns data

                Server srv = new Server(new ServerConnection(new SqlConnection(clientSession.ConnectionString)));
                Database database = srv.Databases[srv.ConnectionContext.DatabaseName];

                // this should never happen, because table name in database is in the zCustom_{XianID} format
                // so, yes client can have duplicate table names (Client readable)
                if (database.Tables.Contains(tableName))
                {
                    Table myTable = database.Tables[tableName];
                    ClientDefinedTable.AddSystemColumns(myTable);
                    myTable.Alter();
                }
                srv.ConnectionContext.Disconnect();
                #endregion

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConnection))
                {
                    try
                    {
                        bulkCopy.BulkCopyTimeout = TimeLimitInSecs;

                        bulkCopy.DestinationTableName = "dbo." + tableName;

                        bulkCopy.WriteToServer(tableWithData);
                    }
                    catch (Exception ex)
                    {
                        //handle error by providing better error messages and context
                        try
                        {
                            SystemException bulkCopyException = new SystemException("failed to append data to target table because " + ex.Message, ex);

                            bulkCopyException.AddContext(("ConnectionString : " + sqlConnection.ConnectionString) + Environment.NewLine
                                                        + ("ConnectionTimeout : " + sqlConnection.ConnectionTimeout)
                                                        + ("DestinationTableName : " + bulkCopy.DestinationTableName) + Environment.NewLine
                                                        + ("BatchSize : " + bulkCopy.BatchSize) + Environment.NewLine
                                                        + ("SourceDataSize (cols * rows) :" + tableWithData.Columns.Count + " * " + tableWithData.Rows.Count) + Environment.NewLine
                                                        );
                            throw bulkCopyException;
                        }
                        catch (Exception)
                        {
                            throw ex;
                        }
                    }

                }
            }
        }


        /// <summary>
        /// Delete the sql table of this CDT
        /// </summary>
        /// <param name="clientSession">client session which has the client id and user name</param>
        /// <param name="XianID">xian id for the CDT to be deleted</param>
        /// <param name="TimeLimitInSecs">Set the time out of truncating sql table</param>
        /// <param name="tableName"></param>
        public static long TruncateSqlTable(Session clientSession, int XianID, int TimeLimitInSecs, string tableName = null)
        {
            string connectionString = clientSession.IsClientDatabaseOverridePresent ? clientSession.ClientDatabaseOverrideConnectionString : RuntimeSettings.LookupClientDbConnectionString(clientSession.Username);

            Server srv = new Server(new ServerConnection(new SqlConnection(connectionString)));

            srv.ConnectionContext.StatementTimeout = TimeLimitInSecs;

            Database database = srv.Databases[srv.ConnectionContext.DatabaseName];
            tableName = ClientDefinedTable.GetSystemTableFromXianID(XianID,false,false);
            var table = database.Tables[tableName, "co"];
            long rowcount = table.RowCount;

            table.TruncateData();

            srv.ConnectionContext.Disconnect();

            return rowcount;
        }

        /// <summary>
        /// Delete the sql table of this CDT
        /// </summary>
        /// <param name="clientSession">client session which has the client id and user name</param>
        /// <param name="XianID">xian id for the CDT to be deleted</param>
        public static void DeleteSqlTable(Session clientSession, int XianID)
        {
            string connectionString = clientSession.IsClientDatabaseOverridePresent ? clientSession.ClientDatabaseOverrideConnectionString : RuntimeSettings.LookupClientDbConnectionString(clientSession.Username);

            Server srv = new Server(new ServerConnection(new SqlConnection(connectionString)));

            Database database = srv.Databases[srv.ConnectionContext.DatabaseName];
            string tableName = ClientDefinedTable.GetSystemTableFromXianID(XianID);

            if (database.Tables.Contains(tableName)) database.Tables[tableName].Drop();
            else if (database.Views.Contains(tableName)) database.Views[tableName].Drop();

            srv.ConnectionContext.Disconnect();
        }

    }
}
