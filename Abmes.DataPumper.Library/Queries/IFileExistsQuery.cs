using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Queries
{
    public interface IFileExistsQuery
    {
        bool FileExists(string fileName, string directoryName);
        Task<bool> FileExistsAsync(string fileName, string directoryName, CancellationToken cancellationToken);
    }
}