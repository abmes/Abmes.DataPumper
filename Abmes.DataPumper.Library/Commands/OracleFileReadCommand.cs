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
    public class OracleFileReadCommand : IFileReadCommand, IDisposable
    {
        public const int MaxChunkSize = 32000;

        private readonly IDataPumperDbConnection _dbConnection;

        private OracleCommand _command;
        private OracleParameter _byteCountParam;
        private OracleParameter _dataOutParam;

        public OracleFileReadCommand(IDataPumperDbConnection dbConnection)
        {
            _dbConnection = dbConnection;
        }

        private void EnsureCommandCreated()
        {
            if (_command == null)
            {
                var sproc = "DataPumperUtils.ReadFromFile";

                _command = new OracleCommand(sproc, _dbConnection.OracleConnection);
                _command.CommandType = CommandType.StoredProcedure;
                _command.InitialLOBFetchSize = MaxChunkSize * 2;
                _command.InitialLONGFetchSize = MaxChunkSize * 2;
                _command.FetchSize = MaxChunkSize * 2;

                _byteCountParam = _command.Parameters.Add(new OracleParameter("AByteCount", MaxChunkSize));
                _dataOutParam = _command.Parameters.Add(new OracleParameter("AData", OracleDbType.Raw, size: MaxChunkSize, obj: null, direction: ParameterDirection.Output));
            }
        }

        public int ByteCountToRead
        {
            get
            {
                EnsureCommandCreated();
                return (int)_byteCountParam.Value;
            }
            set
            {
                EnsureCommandCreated();
                _byteCountParam.Value = value;
            }
        }

        public int ResultDataLength
        {
            get
            {
                EnsureCommandCreated();
                return (((OracleBinary)_dataOutParam.Value).IsNull) ?
                    0 :
                    ((OracleBinary)_dataOutParam.Value).Length;
            }
        }

        public void CopyResultDataTo(byte[] buffer, int offset)
        {
            EnsureCommandCreated();
            ((OracleBinary)_dataOutParam.Value).Value.CopyTo(buffer, offset);
        }

        public Task CopyResultDataToAsync(byte[] buffer, int offset, CancellationToken cancellationToken)
        {
            return Task.Run(() => CopyResultDataTo(buffer, offset), cancellationToken);
        }

        public void Execute()
        {
            EnsureCommandCreated();

            _dataOutParam.Size = ByteCountToRead;
            _dataOutParam.Value = null;

            _command.ExecuteNonQuery();
        }

        public async Task ExecuteAsync(CancellationToken cancellationToken)
        {
            EnsureCommandCreated();

            _dataOutParam.Size = ByteCountToRead;
            _dataOutParam.Value = null;

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
