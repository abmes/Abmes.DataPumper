using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public interface IDataPumperDbConnection
    {
        OracleConnection OracleConnection { get; }
    }
}
