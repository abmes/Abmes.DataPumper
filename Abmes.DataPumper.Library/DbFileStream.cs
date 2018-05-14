using Abmes.DataPumper.Library.Commands;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class DbFileStream : Stream
    {
        private const int _maxReadChunkSize = 32000;
        private const int _maxWriteChunkSize = 32000;

        private readonly IFileOpenCommand _openCommand;
        private readonly IFileCloseCommand _closeCommand;
        private readonly IFileReadCommand[] _readCommands;
        private readonly IFileWriteCommand[] _writeCommands;

        private readonly FileAccessMode _mode;
        private readonly string _fileName;
        private readonly string _directoryName;

        private bool _isOpen;

        public DbFileStream(
            IFileOpenCommand openCommand,
            IFileCloseCommand closeCommand,
            IFileReadCommand readCommand0,
            IFileReadCommand readCommand1,
            IFileWriteCommand writeCommand0,
            IFileWriteCommand writeCommand1,
            FileAccessMode mode,
            string fileName,
            string directoryName = null)
        {
            Contract.Requires((mode == FileAccessMode.Write) || (readCommand0 != readCommand1));

            _openCommand = openCommand;
            _closeCommand = closeCommand;
            _readCommands = new[] { readCommand0, readCommand1 };
            _writeCommands = new[] { writeCommand0, writeCommand1 };

            _fileName = fileName;
            _directoryName = directoryName;
            _mode = mode;
        }

        public override bool CanRead => _mode == FileAccessMode.Read;

        public override bool CanSeek => false;

        public override bool CanWrite => _mode == FileAccessMode.Write;

        public override long Length
        {
            get
            {
                throw new NotImplementedException();  // no need to implement
            }
        }

        public override long Position
        {
            get
            {
                throw new NotImplementedException();  // no need to implement
            }

            set
            {
                throw new NotImplementedException();  // no need to implement
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();  // no need to implement
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();  // no need to implement
        }

        public override void Flush()
        {
            // do nothing - every write flushes
        }

        private void EnsureFileOpen()
        {
            if (!_isOpen)
            {
                _openCommand.Mode = _mode;
                _openCommand.FileName = _fileName;
                _openCommand.DirectoryName = _directoryName;

                _openCommand.Execute();

                _isOpen = true;
            }
        }

        private async Task EnsureFileOpenAsync(CancellationToken cancellationToken)
        {
            if (!_isOpen)
            {
                _openCommand.Mode = _mode;
                _openCommand.FileName = _fileName;
                _openCommand.DirectoryName = _directoryName;

                await _openCommand.ExecuteAsync(cancellationToken);

                _isOpen = true;
            }
        }

        private void EnsureFileClosed()
        {
            if (_isOpen)
            {
                _closeCommand.Execute();
                _isOpen = false;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Contract.Requires(_mode == FileAccessMode.Read);

            EnsureFileOpen();

            var readByteCount = 0;
            var currentOffset = offset;
            var remainingByteCount = count;
            while (remainingByteCount > 0)
            {
                var chunkSize = (remainingByteCount < _maxReadChunkSize) ? remainingByteCount : _maxReadChunkSize;

                _readCommands[0].ByteCountToRead = chunkSize;
                _readCommands[0].Execute();

                var chunkReadSize = _readCommands[0].ResultDataLength;

                if (chunkReadSize == 0)
                {
                    break;
                }

                _readCommands[0].CopyResultDataTo(buffer, currentOffset);

                readByteCount += chunkReadSize;
                currentOffset += chunkReadSize;
                remainingByteCount -= chunkReadSize;
            }

            return readByteCount;
        }
        
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Contract.Requires(_mode == FileAccessMode.Read);

            await EnsureFileOpenAsync(cancellationToken);

            var writeTask = Task.CompletedTask;
            var commandIndex = 0;

            var readByteCount = 0;
            var currentOffset = offset;
            var remainingByteCount = count;
            while (remainingByteCount > 0)
            {
                var chunkSize = (remainingByteCount < _maxReadChunkSize) ? remainingByteCount : _maxReadChunkSize;

                _readCommands[commandIndex].ByteCountToRead = chunkSize;
                var readTask = _readCommands[commandIndex].ExecuteAsync(cancellationToken);

                await Task.WhenAll(readTask, writeTask);

                var chunkReadSize = _readCommands[commandIndex].ResultDataLength;

                if (chunkReadSize == 0)
                {
                    break;
                }

                writeTask = _readCommands[commandIndex].CopyResultDataToAsync(buffer, currentOffset, cancellationToken);

                readByteCount += chunkReadSize;
                currentOffset += chunkReadSize;
                remainingByteCount -= chunkReadSize;

                commandIndex = 1 - commandIndex;
            }

            await writeTask;

            return readByteCount;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Debug.Assert(_mode == FileAccessMode.Write);

            EnsureFileOpen();

            var currentOffset = offset;
            var remainingByteCount = count;
            while (remainingByteCount > 0)
            {
                var chunkSize = (remainingByteCount < _maxWriteChunkSize) ? remainingByteCount : _maxWriteChunkSize;

                _writeCommands[0].CopyDataFrom(buffer, currentOffset, chunkSize);
                _writeCommands[0].Execute();

                currentOffset += chunkSize;
                remainingByteCount -= chunkSize;
            }
        }

        public override async Task WriteAsync(Byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Debug.Assert(_mode == FileAccessMode.Write);

            await EnsureFileOpenAsync(cancellationToken);

            var writeTask = Task.CompletedTask;
            var commandIndex = 0;

            var currentOffset = offset;
            var remainingByteCount = count;
            while (remainingByteCount > 0)
            {
                var chunkSize = (remainingByteCount < _maxWriteChunkSize) ? remainingByteCount : _maxWriteChunkSize;

                var readTask = _writeCommands[commandIndex].CopyDataFromAsync(buffer, currentOffset, chunkSize, cancellationToken);

                await Task.WhenAll(readTask, writeTask);

                writeTask = _writeCommands[commandIndex].ExecuteAsync(cancellationToken);

                currentOffset += chunkSize;
                remainingByteCount -= chunkSize;

                commandIndex = 1 - commandIndex;
            }

            await writeTask;
        }

        protected override void Dispose(bool disposing)
        {
            EnsureFileClosed();
            base.Dispose(disposing);
        }
    }
}
