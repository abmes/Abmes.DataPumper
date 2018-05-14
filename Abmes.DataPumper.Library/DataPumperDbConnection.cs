using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class DataPumperDbConnection : IDataPumperDbConnection, IDisposable
    {
        private readonly ISqlConnectionStringProvider _connectionStringProvider;

        private OracleConnection _oracleConnection;
        private bool _inEnsureInitialized;
        private bool _initialized;

        public DataPumperDbConnection(
            ISqlConnectionStringProvider connectionStringProvider)
        {
            _connectionStringProvider = connectionStringProvider;
        }

        public OracleConnection OracleConnection
        {
            get
            {
                EnsureInitialized();
                return _oracleConnection;
            }
        }

        private void EnsureInitialized()
        {
            if (_inEnsureInitialized)
                return;

            _inEnsureInitialized = true;
            try
            {
                if (_initialized)
                    return;

                var connectionString = _connectionStringProvider.GetConnectionString();

                Debug.Assert(!string.IsNullOrEmpty(connectionString));

                _oracleConnection = new OracleConnection(connectionString);

                _oracleConnection.Open();

                _initialized = true;
            }
            finally
            {
                _inEnsureInitialized = false;
            }
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
                    _oracleConnection?.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        // ~DataPumperDbConnection() {
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
