using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Queries
{
    public class AmazonOracleGetFilesQuery : IGetFilesQuery
    {
        private readonly IDataPumperDbConnection _dbConnection;

        public AmazonOracleGetFilesQuery(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        private OracleCommand CreateOracleCommand(string directoryName)
        {
            var sql = "select f.FILENAME, f.FILESIZE, f.MTIME from table(RDSADMIN.RDS_FILE_UTIL.LISTDIR(:DIRECTORY_NAME)) f where (f.TYPE = 'file') order by f.MTIME";

            var command = new OracleCommand(sql, _dbConnection.OracleConnection);
            try
            {
                command.Parameters.Add(new OracleParameter("DIRECTORY_NAME", directoryName));
                return command;
            }
            catch
            {
                command.Dispose();
                throw;
            }
        }

        public IEnumerable<FileInfo> GetFiles(string directoryName)
        {
            using (var command = CreateOracleCommand(directoryName))
            {
                using (var reader = command.ExecuteReader())
                {
                    return GetResult(reader).ToList();  // bez toList se zatvaria readera
                }
            }
        }

        public async Task<IEnumerable<FileInfo>> GetFilesAsync(string directoryName, CancellationToken cancellationToken)
        {
            using (var command = CreateOracleCommand(directoryName))
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    return GetResult(reader).ToList();  // bez toList se zatvaria readera
                }
            }
        }

        private IEnumerable<FileInfo> GetResult(DbDataReader reader)
        {
            if (reader.HasRows)
            {
                while (reader.Read())  // async ne raboti v kombinacia s yield
                {
                    yield return new FileInfo(reader.GetString(0), reader.GetInt64(1), reader.GetDateTime(2));
                }
            }
        }
    }
}
