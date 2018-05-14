using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Queries
{
    public interface IGetFilesQuery
    {
        IEnumerable<FileInfo> GetFiles(string directoryName);
        Task<IEnumerable<FileInfo>> GetFilesAsync(string directoryName, CancellationToken cancellationToken);
    }
}