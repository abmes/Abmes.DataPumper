using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Queries
{
    public class OracleFileExistsQuery : IFileExistsQuery
    {
        private readonly IDataPumperDbConnection _dbConnection;

        public OracleFileExistsQuery(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        private OracleCommand CreateOracleCommand(string fileName, string directoryName)
        {
            var sproc = "DataPumperUtils.FileExistsNum";

            var command = new OracleCommand(sproc, _dbConnection.OracleConnection);
            try
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new OracleParameter("Result", OracleDbType.Int32, ParameterDirection.ReturnValue));
                command.Parameters.Add(new OracleParameter("AFileName", fileName));
                command.Parameters.Add(new OracleParameter("ADirectoryName", directoryName));
                return command;
            }
            catch
            {
                command.Dispose();
                throw;
            }
        }

        public bool FileExists(string fileName, string directoryName)
        {
            using (var command = CreateOracleCommand(fileName, directoryName))
            {
                command.ExecuteNonQuery();
                return GetResult(command);
            }
        }

        public async Task<bool> FileExistsAsync(string fileName, string directoryName, CancellationToken cancellationToken)
        {
            using (var command = CreateOracleCommand(fileName, directoryName))
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
                return GetResult(command);
            }
        }

        private bool GetResult(OracleCommand command)
        {
            var resultParam = command.Parameters["Result"];

            if ((resultParam.Value == null) || (((OracleDecimal)resultParam.Value)).IsNull)
                return false;
            else
                return !((OracleDecimal)resultParam.Value).IsZero;
        }
    }
}
