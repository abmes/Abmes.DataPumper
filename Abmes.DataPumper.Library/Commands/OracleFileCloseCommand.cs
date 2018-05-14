using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public class OracleFileCloseCommand : IFileCloseCommand
    {
        private readonly IDataPumperDbConnection _dbConnection;

        public OracleFileCloseCommand(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        private OracleCommand CreateOracleCommand()
        {
            var sproc = "DataPumperUtils.CloseFile";

            var command = new OracleCommand(sproc, _dbConnection.OracleConnection);
            try
            {
                command.CommandType = CommandType.StoredProcedure;
                return command;
            }
            catch
            {
                command.Dispose();
                throw;
            }
        }

        public void Execute()
        {
            using (var command = CreateOracleCommand())
            {
                command.ExecuteNonQuery();
            }
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            using (var command = CreateOracleCommand())
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }
}
