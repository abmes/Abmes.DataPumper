using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.WebApi.Utils
{
    public class ReadOnlyStreamWrapper : Stream
    {
        private readonly Stream _stream;
        private long _position;

        public ReadOnlyStreamWrapper(Stream stream)
        {
            _stream = stream;
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;

        public override long Position
        {
            get { return _position; }
            set
            {
                throw new NotSupportedException();
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            _position += count;
            return _stream.Read(buffer, offset, count);
        }

        public override int ReadByte()
        {
            _position += 1;
            return _stream.ReadByte();
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            _position += count;
            return _stream.ReadAsync(buffer, offset, count, cancellationToken);
        }

        // Crap that we just have to forward.

        public override bool CanTimeout => _stream.CanTimeout;
        public override int ReadTimeout
        {
            get { return _stream.ReadTimeout; }
            set { _stream.ReadTimeout = value; }
        }
        public override int WriteTimeout
        {
            get { return _stream.WriteTimeout; }
            set { _stream.WriteTimeout = value; }
        }

        public override void Flush() => _stream.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _stream.FlushAsync(cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _stream.Dispose();
        }

        public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            return _stream.CopyToAsync(destination, bufferSize, cancellationToken);
        }


        // Unsupported operations.

        public override long Length
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            if (origin == SeekOrigin.End)
            {
                throw new NotSupportedException("Seek from end not supported");
            }

            long toRead;
            if (origin == SeekOrigin.Begin)
            {
                toRead = offset - Position;
            }
            else
            {
                Debug.Assert(origin == SeekOrigin.Current);
                toRead = offset;
            }

            if (toRead < 0)
            {
                throw new NotSupportedException("Seek backwards not supported");
            }

            const int bufferSize = 16384;

            var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            try
            {
                while (toRead > 0)
                {
                    var chunkSize = (toRead < bufferSize) ? (int)toRead : bufferSize;
                    var chunkReadSize = Read(buffer, 0, chunkSize);

                    if (chunkReadSize == 0)
                    {
                        break;
                    }

                    toRead -= chunkReadSize;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }

            return Position;
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }
    }
}
