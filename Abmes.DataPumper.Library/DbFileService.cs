using Abmes.DataPumper.Library.Commands;
using Abmes.DataPumper.Library.Queries;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class DbFileService : IDbFileService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly IFileExistsQuery _fileExsistsQuery;
        private readonly IFileDeleteCommand _fileDeleteCommand;
        private readonly IGetFilesQuery _getFilesQuery;

        public DbFileService(IServiceProvider serviceProvider, IFileExistsQuery fileExsistsQuery, IFileDeleteCommand fileDeleteCommand, IGetFilesQuery getFilesQuery)
        {
            _serviceProvider = serviceProvider;
            _fileExsistsQuery = fileExsistsQuery;
            _fileDeleteCommand = fileDeleteCommand;
            _getFilesQuery = getFilesQuery;
        }

        public async Task<IEnumerable<FileInfo>> GetFilesAsync(string directoryName, CancellationToken cancellationToken)
        {
            return await _getFilesQuery.GetFilesAsync(directoryName, cancellationToken);
        }

        public async Task<bool> FileExistsAsync(string fileName, string directoryName, CancellationToken cancellationToken)
        {
            return await _fileExsistsQuery.FileExistsAsync(fileName, directoryName, cancellationToken);
        }

        public async Task DeleteFileAsync(string fileName, string directoryName, CancellationToken cancellationToken)
        {
            _fileDeleteCommand.FileName = fileName;
            _fileDeleteCommand.DirectoryName = directoryName;
            await _fileDeleteCommand.ExecuteAsync(cancellationToken);
        }

        public Stream GetFileReadStream(string fileName, string directoryName = null)
        {
            return new DbFileStream(
                (IFileOpenCommand)_serviceProvider.GetService(typeof(IFileOpenCommand)),  // ne se polzva generichen metod za da ne se dobavia dependency wyrhu dopylnitelno asembli za DI
                (IFileCloseCommand)_serviceProvider.GetService(typeof(IFileCloseCommand)),
                (IFileReadCommand)_serviceProvider.GetService(typeof(IFileReadCommand)),
                (IFileReadCommand)_serviceProvider.GetService(typeof(IFileReadCommand)),
                null,
                null,
                FileAccessMode.Read,
                fileName,
                directoryName);
        }

        public Stream GetFileWriteStream(string fileName, string directoryName = null)
        {
            return new DbFileStream(
                (IFileOpenCommand)_serviceProvider.GetService(typeof(IFileOpenCommand)),
                (IFileCloseCommand)_serviceProvider.GetService(typeof(IFileCloseCommand)),
                null,
                null,
                (IFileWriteCommand)_serviceProvider.GetService(typeof(IFileWriteCommand)),
                (IFileWriteCommand)_serviceProvider.GetService(typeof(IFileWriteCommand)),
                FileAccessMode.Write,
                fileName,
                directoryName);
        }
    }
}
