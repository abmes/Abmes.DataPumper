using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public interface IExporter
    {
        Task StartExportSchemaAsync(string schemaName, string dumpFileName, string logFileName, string directoryName, string dumpFileSize, CancellationToken cancellationToken);
        Task<ExportLogData> GetExportLogDataAsync(string schemaName, string logFileName, string directoryName, CancellationToken cancellationToken);
    }
}
