using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class OracleImporter : IImporter
    {
        private readonly IDataPumperDbConnection _dbConnection;

        public OracleImporter(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        public async Task StartImportSchemaAsync(
            string fromSchemaName, 
            string toSchemaName, 
            string toSchemaPassword, 
            string dumpFileName, 
            string logFileName, 
            string directoryName, 
            CancellationToken cancellaionToken)
        {
            var sql =
                "begin" + Environment.NewLine +
                "  DataPumperUtils.StartImportSchema(:FROM_SCHEMA_NAME, :TO_SCHEMA_NAME, :TO_SCHEMA_PASSWORD, :DUMP_FILE_NAME, :LOG_FILE_NAME, :DIRECTORY_NAME);" + Environment.NewLine +
                "end;";

            using (var command = new OracleCommand(sql, _dbConnection.OracleConnection))
            {
                command.Parameters.Add(new OracleParameter("FROM_SCHEMA_NAME", fromSchemaName));
                command.Parameters.Add(new OracleParameter("TO_SCHEMA_NAME", toSchemaName));
                command.Parameters.Add(new OracleParameter("TO_SCHEMA_PASSWORD", toSchemaPassword));
                command.Parameters.Add(new OracleParameter("DUMP_FILE_NAME", dumpFileName));
                command.Parameters.Add(new OracleParameter("LOG_FILE_NAME", logFileName));
                command.Parameters.Add(new OracleParameter("DIRECTORY_NAME", directoryName));
                await command.ExecuteNonQueryAsync(cancellaionToken);
            }
        }
    }
}
