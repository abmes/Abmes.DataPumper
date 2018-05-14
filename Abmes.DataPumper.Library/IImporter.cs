using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public interface IImporter
    {
        Task StartImportSchemaAsync(
            string fromSchemaName, 
            string toSchemaName, 
            string toSchemaPassword, 
            string dumpFileName, 
            string logFileName, 
            string directoryName, 
            CancellationToken cancellationToken);
    }
}
