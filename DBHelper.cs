using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlTypes;
using solvexia.svx.server.lib.utilities;
using solvexia.svx.shared.lib.compression;
using solvexia.svx.shared.lib.encryption;
using svx.script.filemigration.Models;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;

namespace svx.script.filemigration
{
    internal class DBHelper
    {
        private readonly CryptoHelper _cryptoHelper;
        private readonly ChecksumHelper _checksumHelper;

        public DBHelper(CryptoHelper cryptoHelper, ChecksumHelper checksumHelper)
        {
            _cryptoHelper = cryptoHelper;
            _checksumHelper = checksumHelper;
        }

        public async Task SetAzBlobStorageAsDefaultAndWipeFileStreamData(SqlConnection connection)
        {
            using var command = new SqlCommand();
            command.CommandType = CommandType.StoredProcedure;
            command.CommandText = "[dbo].[ControlData_Set]";
            command.Parameters.Add(GetSqlParameter("@Group", "System", SqlDbType.NVarChar, ParameterDirection.Input, -1));
            command.Parameters.Add(GetSqlParameter("@Item", "DefaultFileStorageProviderId", SqlDbType.NVarChar, ParameterDirection.Input, -1));
            command.Parameters.Add(GetSqlParameter("@Value", "2", SqlDbType.NVarChar, ParameterDirection.Input, -1));
            command.Parameters.Add(GetSqlParameter("@RefreshPeriodInSeconds", 120, SqlDbType.Int, ParameterDirection.Input, -1));
            command.Connection = connection;
            
            await command.ExecuteNonQueryAsync();

            using var command2 = new SqlCommand();
            command2.Connection = connection;
            command2.CommandTimeout = 300;
            command2.Parameters.Add(GetSqlParameter("@CompressionSchemeID", 0.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));
            command2.Parameters.Add(GetSqlParameter("@EncryptionSchemeID", 0.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));

            command2.CommandText = $"UPDATE XianFiles " +
                $"                      SET [Data]=0x00, " +
                $"                          [CompressionSchemeID]=0, " +
                $"                          [EncryptionSchemeID]=0 " +
               $"                 WHERE FileStorageProviderId=2 AND XianID NOT IN (SELECT distinct XianID FROM Temp_XianFilesMigration WHERE StagedUrl is null);" +
                $"                 UPDATE XianFiles_History " +
                $"                      SET OldData=0x00, " +
                $"                          [CompressionSchemeID]=0, " +
                $"                          [EncryptionSchemeID]=0 " +
                $"                 WHERE FileStorageProviderId=2 AND XianID NOT IN (SELECT distinct XianID FROM Temp_XianFilesMigration WHERE StagedUrl is null);";

            await command2.ExecuteNonQueryAsync();
        }

        public async Task<int> GetTotalFilesToMigrate(SqlConnection connection)
        {
            using var command = new SqlCommand();
            command.CommandText = "SELECT (a.XFCount + b.XFHCount) FROM " +
                                        "(SELECT COUNT(x.XianID) AS XFCount " +
                                                "FROM Xian x, XianFiles xf " +
                                                "WHERE x.XianTypeID = 8 AND IsType = 0 AND x.DateDeleted IS NULL AND " +
                                                "xf.XianID = x.XianID AND xf.FileState = 1 AND xf.FileStorageProviderID = 1) a, " +
                                        "(SELECT COUNT(*) AS XFHCount FROM (SELECT distinct xianid, version FROM " +
                                                "XianFiles_History WHERE FileStorageProviderID = 1 AND XianID IN(SELECT x.XianID " +
                                                                                    "FROM Xian x, XianFiles xf " +
                                                                                    "WHERE x.XianTypeID = 8 AND IsType = 0 AND x.DateDeleted IS NULL AND " +
                                                                                    "xf.XianID = x.XianID AND xf.FileState = 1 AND xf.FileStorageProviderID = 1)) subB) b";
            command.Connection = connection;
            var totalFiles = await command.ExecuteScalarAsync();
            return (int)totalFiles;
        }

        public async Task<int> GetTotalFilesMigrated(SqlConnection connection)
        {
            using var command = new SqlCommand();
            command.CommandText = "SELECT COUNT(*) FROM Temp_XianFilesMigration WHERE [DateMigrated] IS NOT NULL";
            command.Connection = connection;
            var totalFilesMigrated = await command.ExecuteScalarAsync();
            return (int)totalFilesMigrated;
        }

        public async Task<(bool Success, string ErrorMessage)> UpdateTemp_XianFilesMigrationRecord(SqlConnection connection, int xianId, int version, string stagedBlobUrl, string error, DateTime? dateMigrated)
        {
            using var transaction = connection.BeginTransaction();
            try
            {
                using var command = new SqlCommand();
                command.Transaction = transaction;
                command.Parameters.Add(GetSqlParameter("@XianId", xianId.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));
                command.Parameters.Add(GetSqlParameter("@Version", version.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));

                command.Parameters.Add(GetSqlParameter("@StagedBlobUrl", stagedBlobUrl.GetValueForSqlParameter(), SqlDbType.NVarChar, ParameterDirection.Input, 1024));
                command.Parameters.Add(GetSqlParameter("@Error", error.GetValueForSqlParameter(), SqlDbType.NVarChar, ParameterDirection.Input, 4000));
                command.Parameters.Add(GetSqlParameter("@DateMigrated", dateMigrated.GetValueForSqlParameter(), SqlDbType.DateTime2, ParameterDirection.Input));

                var maxExpectedUpdateCount = 1;
                var updateXianFilesSql = string.Empty;

                updateXianFilesSql = $"UPDATE [Temp_XianFilesMigration] " +
                    $"                      SET StagedUrl=@StagedBlobUrl, " +
                    $"                          [Error]=@Error, " +
                    $"                          DateMigrated=@DateMigrated" +
                    $"                 WHERE XianID=@XianId AND" +
                    $"                       [Version]=@Version;";

                command.CommandText = updateXianFilesSql;
                command.Connection = connection;

                var updatedRecords = await command.ExecuteNonQueryAsync();
                if (updatedRecords > maxExpectedUpdateCount)
                {
                    transaction.Rollback();
                    return (false, $"Method UpdateTemp_XianFilesMigrationRecord:{Environment.NewLine}Maximum expected row update count was {maxExpectedUpdateCount}, but actual row update count {updatedRecords}");
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                return (false, $"Method UpdateTemp_XianFilesMigrationRecord:{Environment.NewLine}Exception type: {ex.GetType()}{Environment.NewLine}Exception message: {ex.Message}{Environment.NewLine}Stack trace: {ex.StackTrace}");
            }
            return (true, string.Empty);
        }

        public async Task<(bool Success, string ErrorMessage)> UpdateXianFilesOrHistoryRecord(SqlConnection connection, int xianId, int? version, bool isCurrentVersion, string blobUrl, byte[] md5Hash)
        {
            using var transaction = connection.BeginTransaction();
            try
            {
                using var command = new SqlCommand();
                command.Transaction = transaction;
                command.Parameters.Add(GetSqlParameter("@XianId", xianId.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));
                command.Parameters.Add(GetSqlParameter("@CompressionSchemeID", 0.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));
                command.Parameters.Add(GetSqlParameter("@EncryptionSchemeID", 0.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));
                command.Parameters.Add(GetSqlParameter("@Location", blobUrl.GetValueForSqlParameter(), SqlDbType.NVarChar, ParameterDirection.Input, 1024));
                command.Parameters.Add(GetSqlParameter("@FileStorageProviderId", 2.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input));
                command.Parameters.Add(GetSqlParameter("@Md5Hash", md5Hash.GetValueForSqlParameter(), SqlDbType.Binary, ParameterDirection.Input, 16));

                var maxExpectedUpdateCount = 0;
                var updateXianFilesSql = string.Empty;
                if (isCurrentVersion)
                {
                    maxExpectedUpdateCount = 2;
                    command.Parameters.Add(GetSqlParameter("@FileState1", 1, SqlDbType.TinyInt, ParameterDirection.Input));
                    command.Parameters.Add(GetSqlParameter("@FileState0", 0, SqlDbType.TinyInt, ParameterDirection.Input));

                    updateXianFilesSql = $"UPDATE XianFiles " +
                        $"                      SET [Location]=null, " +
                        $"                          FileStorageProviderId=@FileStorageProviderId," +
                        $"                          MD5Hash=null" +
                        $"                 WHERE XianID=@XianId AND" +
                        $"                       FileState=@FileState0;" +
                        $"                 UPDATE XianFiles " +
                        $"                      SET [Location]=@Location, " +
                        $"                          FileStorageProviderId=@FileStorageProviderId," +
                        $"                          MD5Hash=@Md5Hash" +
                        $"                 WHERE XianID=@XianId AND " +
                        $"                       FileState=@FileState1;";
                }
                else
                {
                    maxExpectedUpdateCount = 1;
                    command.Parameters.Add(GetSqlParameter("@Version", version, SqlDbType.Int, ParameterDirection.Input));

                    updateXianFilesSql = $"UPDATE XianFiles_History " +
                        $"                      SET [Location]=@Location, " +
                        $"                          FileStorageProviderId=@FileStorageProviderId, " +
                        $"                          MD5Hash=@Md5Hash" +
                        $"                 WHERE XianID=@XianId AND" +
                        $"                       [Version]=@Version;";
                }

                command.CommandText = updateXianFilesSql;
                command.Connection = connection;

                var updatedRecords = await command.ExecuteNonQueryAsync();
                if (updatedRecords > maxExpectedUpdateCount)
                {
                    transaction.Rollback();
                    return (false, $"Method UpdateXianFilesOrHistoryRecord:{Environment.NewLine}Maximum expected row update count was {maxExpectedUpdateCount}, but actual row update count {updatedRecords}");
                }

                transaction.Commit();
            }
            catch(Exception ex)
            {
                transaction.Rollback();
                return (false, $"Method UpdateXianFilesOrHistoryRecord:{Environment.NewLine}Exception type: {ex.GetType()}{Environment.NewLine}Exception message: {ex.Message}{Environment.NewLine}Stack trace: {ex.StackTrace}");
            }
            return (true, string.Empty);
        }

        public async Task<(bool Success, string ErrorMessage)> UpsertAzureFileStorageConfig(SqlConnection connection, string containerUrl, string encryptedSasKey)
        {
            var transaction = connection.BeginTransaction();

            var upsertFileStorageProvider = @"MERGE INTO FileStorageProvider t
                                                        USING (SELECT 2 AS FileStorageProviderId, 2 AS FileStorageProviderTypeId, @ContainerUrl AS BaseLocation, 'Azure blob storage' AS Name) s
                                                        ON (s.FileStorageProviderId = t.FileStorageProviderId)
                                                        WHEN MATCHED THEN
	                                                        UPDATE SET t.BaseLocation = s.BaseLocation,
			                                                            t.Name = s.Name
                                                        WHEN NOT MATCHED BY TARGET THEN
	                                                        INSERT (FileStorageProviderId,FileStorageProviderTypeId,BaseLocation,Name)
	                                                        VALUES (s.FileStorageProviderId,s.FileStorageProviderTypeId,s.BaseLocation,s.Name);";
                                            
            var upsertFileStorageProviderAzure =  @"MERGE INTO FileStorageProviderAzure t
                                                        USING (SELECT 2 AS FileStorageProviderId, @EncryptedKey AS [Key], 2 AS FileStorageProviderAzureKeyTypeID, 'svx' AS AccessPolicyID, '2020-04-08' AS SASVersion) s
                                                        ON (s.FileStorageProviderId = t.FileStorageProviderId)
                                                        WHEN MATCHED THEN
	                                                        UPDATE SET t.[Key] = s.[Key],
			                                                           t.SASVersion = s.SASVersion,
			                                                           t.AccessPolicyID = s.AccessPolicyID
                                                        WHEN NOT MATCHED BY TARGET THEN
	                                                        INSERT (FileStorageProviderId,[Key],FileStorageProviderAzureKeyTypeID,AccessPolicyID,SASVersion)
	                                                        VALUES (s.FileStorageProviderId,s.[Key],s.FileStorageProviderAzureKeyTypeID,s.AccessPolicyID,s.SASVersion);";

            try
            {
                using var command = new SqlCommand(upsertFileStorageProvider, connection, transaction);
                command.Parameters.Add(GetSqlParameter("@ContainerUrl", containerUrl.GetValueForSqlParameter(), SqlDbType.NVarChar, ParameterDirection.Input, size: -1));
                
                var updatedRecords = await command.ExecuteNonQueryAsync();
                if (updatedRecords != 1)
                {
                    transaction.Rollback();
                    return (false, $"Updating FileStorageProvider table with Azure blob storage entry did not have exactly one row updated");
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                return (false, $"Failed to update FileStorageProvider table{Environment.NewLine}Method UpsertAzureFileStorageConfig:{Environment.NewLine}Exception type: {ex.GetType()}{Environment.NewLine}Exception message: {ex.Message}{Environment.NewLine}Stack trace: {ex.StackTrace}");
            }

            transaction = connection.BeginTransaction();
            try
            {
                using var command = new SqlCommand(upsertFileStorageProviderAzure, connection, transaction);
                command.Parameters.Add(GetSqlParameter("@EncryptedKey", encryptedSasKey.GetValueForSqlParameter(), SqlDbType.NVarChar, ParameterDirection.Input, size: -1));

                var updatedRecords = await command.ExecuteNonQueryAsync();
                if (updatedRecords != 1)
                {
                    transaction.Rollback();
                    return (false, $"Updating FileStorageProviderAzure table with Azure blob storage config(key) entry did not have exactly one row updated");
                }

                transaction.Commit();
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                return (false, $"Failed to update FileStorageProviderAzure table{Environment.NewLine}Method UpsertAzureFileStorageConfig:{Environment.NewLine}Exception type: {ex.GetType()}{Environment.NewLine}Exception message: {ex.Message}{Environment.NewLine}Stack trace: {ex.StackTrace}");
            }

            return (true, string.Empty);
        }

        public async Task<FileStorageProviderAzure> GetAzureFileStorageProvider(SqlConnection connection)
        {
            var getFileStorageProviderSql = "SELECT     " +
                "                                   fsp.[FileStorageProviderId]," +
                "                                   fspa.[Key],	" +
                "                                   fspa.[FileStorageProviderAzureKeyTypeID]," +
                "                               	fspa.[AccessPolicyID]," +
                "                               	fspa.[SASVersion]," +
                "                               	fsp.[BaseLocation] FROM FileStorageProvider fsp " +
                "                                       INNER JOIN FileStorageProviderAzure fspa " +
                "                                               ON fsp.FileStorageProviderId = fspa.FileStorageProviderId" +
                "                            WHERE fsp.FileStorageProviderId = 2";

            using var command = new SqlCommand(getFileStorageProviderSql, connection);
            using var sqlDataReader = await command.ExecuteReaderAsync();

            if (!sqlDataReader.Read())
                return null;

            return new FileStorageProviderAzure((int)sqlDataReader["FileStorageProviderId"],
                                                (string)sqlDataReader["Key"],
                                                (int)sqlDataReader["FileStorageProviderAzureKeyTypeID"],
                                                (string)sqlDataReader["AccessPolicyID"],
                                                (string)sqlDataReader["BaseLocation"],
                                                (string)sqlDataReader["SASVersion"]);
        }

        public async Task<XianFilesMigration[]> GetNextFilesForMigration(SqlConnection connection, string instanceName)
        {
            var temp_getNextFileForMigration = "Temp_GetNextFileForMigration";

            var xianFilesMigrationSet = new List<XianFilesMigration>();

            using var command = new SqlCommand(temp_getNextFileForMigration, connection);
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add(GetSqlParameter("@InstanceName", instanceName, SqlDbType.NVarChar, ParameterDirection.Input, 128));
            using var sqlDataReader = await command.ExecuteReaderAsync();
            while (sqlDataReader.Read())
            {
                xianFilesMigrationSet.Add(new XianFilesMigration(
                                                    (int)sqlDataReader["XianID"],
                                                    (int)sqlDataReader["Version"],
                                                    (bool)sqlDataReader["IsCurrentRecord"],
                                                    (string)sqlDataReader["InstanceName"],
                                                    (string.IsNullOrEmpty(sqlDataReader["StagedUrl"].ToString())) ? null : (string)sqlDataReader["StagedUrl"],
                                                    (string.IsNullOrEmpty(sqlDataReader["Error"].ToString())) ? null : (string)sqlDataReader["Error"],
                                                    (DateTime)sqlDataReader["DateCreated"],
                                                    (string.IsNullOrEmpty(sqlDataReader["DateMigrated"].ToString())) ? null : (DateTime?)sqlDataReader["DateMigrated"]));
            }

            return xianFilesMigrationSet.ToArray();
        }

        public async Task<(string LocalFilePath, byte[] MD5)> GetFromSqlFilestream(SqlConnection connection, int xianId, int? version, string tempDirectory)
        {
            var fileIdParameter = GetSqlParameter("@XianID", xianId.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input, null);
            var versionParameter = GetSqlParameter("@Version", version.GetValueForSqlParameter(), SqlDbType.Int, ParameterDirection.Input, null);

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            using (var transaction = connection.BeginTransaction())
            {
                try
                {
                    var sql = (version == null)
                        ? "SELECT CompressionSchemeID, EncryptionSchemeID, GET_FILESTREAM_TRANSACTION_CONTEXT(), Data.PathName() FROM XianFiles WHERE XianID = @XianID AND FileState = 1 AND FileStorageProviderID = 1"
                        : "SELECT TOP 1 CompressionSchemeID, EncryptionSchemeID, GET_FILESTREAM_TRANSACTION_CONTEXT(), OldData.PathName() FROM XianFiles_History WHERE XianID = @XianID AND [Version] = @Version AND FileStorageProviderID = 1 ORDER BY [Timestamp] DESC";

                    using (var command = new SqlCommand(sql, connection, transaction))
                    {
                        command.Parameters.Add(fileIdParameter);

                        if (version != null)
                            command.Parameters.Add(versionParameter);
                      
                        using (var sqlReader = await command.ExecuteReaderAsync())
                        {
                            await sqlReader.ReadAsync();

                            var compressionScheme = sqlReader.GetSqlInt32(0).IsNull ? 0 : sqlReader.GetSqlInt32(0).Value;
                            var encryptionSchemeID = sqlReader.GetSqlInt32(1).IsNull ? 0 : sqlReader.GetSqlInt32(1).Value;
                            var transactionContext = sqlReader.GetSqlBinary(2).Value;
                            var fileDataPathName = sqlReader.GetSqlString(3).Value;

                            sqlReader.Close();

                            var localtempfilepath = CreateFileFromSQLPathName(fileDataPathName, transactionContext, tempDirectory);

                            Compressor.DecompressFile(localtempfilepath, ((Compressor.CompressionScheme)compressionScheme));

                            if (encryptionSchemeID > 0)
                            {
                                _cryptoHelper.DecryptFile(localtempfilepath, ((CryptoHelper.EncryptionSchemes)encryptionSchemeID));
                            }

                            transaction.Commit();  // Commit the read transaction, this has to be the last statement in this code block

                            var md5 = _checksumHelper.GetMD5Hash(localtempfilepath);

                            return (localtempfilepath, md5);
                        }
                    }
                }
                catch (Exception)
                {
                    if (transaction != null)
                        transaction.Rollback(); // Release the transaction 

                    throw; // rethrow the exception to be logged and handled ( do not use "throw ex2;" - this will remove the stack trace information )
                }
            }
        }

        private SqlParameter GetSqlParameter(string name, object value, SqlDbType dataType, ParameterDirection? direction = null, int? size = null)
        {
            var sqlParam = new SqlParameter(name, value)
            {
                SqlDbType = dataType
            };

            if (direction != null)
                sqlParam.Direction = direction.Value;

            if (size != null)
                sqlParam.Size = size.Value;

            return sqlParam;
        }

        private string CreateFileFromSQLPathName(string SQLFilestreamPathName, byte[] sqlTransactionContext, string tempDirectory)
        {
            var newfilename = Path.Combine(tempDirectory, Guid.NewGuid().ToString("D"));

            // Because this operation spans potentially a large number of calls over the network - we are placing this in a retry loop to deal with the rare
            // but possible intermittent network failures or database locking issues. We use the defaults for retry (3 times) 

            RetryHelper rth = new RetryHelper();

            rth.Retry(() =>
            {
                using (SqlFileStream stream = new SqlFileStream(SQLFilestreamPathName, sqlTransactionContext, FileAccess.Read, FileOptions.SequentialScan, 0))
                {
                    stream.Flush();// flush any data in the stream buffers to SQL-Server            
                    stream.Seek(0, SeekOrigin.Begin);// rewind the stream to the beginning so that we can access all of the data

                    using (FileStream newfile = new FileStream(newfilename, FileMode.Create))
                    {
                        stream.CopyTo(newfile, FileFunctions.cnst_default_buffersize);
                        newfile.Flush();
                        newfile.Close();
                    }

                    stream.Close();
                }
            });

            return newfilename;
        }
    }
}
