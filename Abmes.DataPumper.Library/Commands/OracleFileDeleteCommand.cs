﻿using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public class OracleFileDeleteCommand : IFileDeleteCommand
    {
        private readonly IDataPumperDbConnection _dbConnection;

        public string FileName { get; set; }

        public string DirectoryName { get; set; }

        public OracleFileDeleteCommand(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        private OracleCommand CreateOracleCommand()
        {
            var sproc = "DataPumperUtils.DeleteFile";

            var command = new OracleCommand(sproc, _dbConnection.OracleConnection);
            try
            {
                command.CommandType = CommandType.StoredProcedure;
                command.Parameters.Add(new OracleParameter("AFileName", FileName));
                command.Parameters.Add(new OracleParameter("ADirectoryName", DirectoryName));
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
