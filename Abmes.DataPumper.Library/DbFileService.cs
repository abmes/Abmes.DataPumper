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
        private readonly Func<IFileOpenCommand> _fileOpenCommandFactory;
        private readonly Func<IFileCloseCommand> _fileCloseCommandFactory;
        private readonly Func<IFileReadCommand> _fileReadCommandFactory;
        private readonly Func<IFileWriteCommand> _fileWriteCommandFactory;
        private readonly IFileExistsQuery _fileExsistsQuery;
        private readonly IFileDeleteCommand _fileDeleteCommand;
        private readonly IGetFilesQuery _getFilesQuery;

        public DbFileService(
            Func<IFileOpenCommand> fileOpenCommandFactory,
            Func<IFileCloseCommand> fileCloseCommandFactory,
            Func<IFileReadCommand> fileReadCommandFactory,
            Func<IFileWriteCommand> fileWriteCommandFactory,
            IFileExistsQuery fileExsistsQuery, 
            IFileDeleteCommand fileDeleteCommand, 
            IGetFilesQuery getFilesQuery)
        {
            _fileOpenCommandFactory = fileOpenCommandFactory;
            _fileCloseCommandFactory = fileCloseCommandFactory;
            _fileReadCommandFactory = fileReadCommandFactory;
            _fileWriteCommandFactory = fileWriteCommandFactory;
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
                _fileOpenCommandFactory(),
                _fileCloseCommandFactory(),
                _fileReadCommandFactory(),
                _fileReadCommandFactory(),
                null,
                null,
                FileAccessMode.Read,
                fileName,
                directoryName);
        }

        public Stream GetFileWriteStream(string fileName, string directoryName = null)
        {
            return new DbFileStream(
                _fileOpenCommandFactory(),
                _fileCloseCommandFactory(),
                null,
                null,
                _fileWriteCommandFactory(),
                _fileWriteCommandFactory(),
                FileAccessMode.Write,
                fileName,
                directoryName);
        }
    }
}
