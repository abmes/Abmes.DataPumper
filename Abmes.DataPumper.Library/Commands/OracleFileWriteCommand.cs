using Oracle.ManagedDataAccess.Client;
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
    public class OracleFileWriteCommand : IFileWriteCommand, IDisposable
    {
        public const int MaxChunkSize = 32000;

        private readonly IDataPumperDbConnection _dbConnection;

        private OracleCommand _command;
        private OracleParameter _dataParam;

        public OracleFileWriteCommand(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        private void EnsureCommandCreated()
        {
            if (_command == null)
            {
                var sproc = "DataPumperUtils.WriteToFile";

                _command = new OracleCommand(sproc, _dbConnection.OracleConnection);
                _command.CommandType = CommandType.StoredProcedure;

                _dataParam = _command.Parameters.Add(new OracleParameter("AData", OracleDbType.Raw, size: MaxChunkSize, obj: null, direction: ParameterDirection.Input));
            }
        }
        
        public void CopyDataFrom(byte[] buffer, int offset, int count)
        {
            EnsureCommandCreated();

            var chunk = new byte[count];
            Buffer.BlockCopy(buffer, offset, chunk, 0, count);
            _dataParam.Value = chunk;
        }

        public Task CopyDataFromAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return Task.Run(() => CopyDataFrom(buffer, offset, count), cancellationToken);
        }

        public void Execute()
        {
            EnsureCommandCreated();

            _command.ExecuteNonQuery();
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            EnsureCommandCreated();

            await _command.ExecuteNonQueryAsync(cancellationToken);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).

                    _command?.Dispose();
                    // dali ne tiabva i parametrite da osvobodim ? dali odp.net sam gi osvobovdava ??
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~ReadCommand() {
        //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
        //   Dispose(false);
        // }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            // GC.SuppressFinalize(this);
        }
        #endregion
    }
}
