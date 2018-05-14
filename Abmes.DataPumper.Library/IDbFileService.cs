using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public interface IDbFileService
    {
        Task<IEnumerable<FileInfo>> GetFilesAsync(string directoryName, CancellationToken cancellationToken);
        Task<bool> FileExistsAsync(string fileName, string directoryName, CancellationToken cancellationToken);
        Task DeleteFileAsync(string fileName, string directoryName, CancellationToken cancellationToken);
        Stream GetFileReadStream(string fileName, string directoryName = null);
        Stream GetFileWriteStream(string fileName, string directoryName = null);
    }
}
