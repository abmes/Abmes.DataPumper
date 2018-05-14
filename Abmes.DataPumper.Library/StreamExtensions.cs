using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public static class StreamExtensions
    {
        public static async Task CopyToParallelAsync(this Stream source, Stream destination, Int32 bufferSize, CancellationToken cancellationToken)
        {
            Contract.Requires(destination != null);
            Contract.Requires(source.CanRead);
            Contract.Requires(destination.CanWrite);

            await ParallelCopy.CopyAsync(
                    (buffer, cancelationToken) => source.ReadAsync(buffer, 0, buffer.Length, cancellationToken),
                    (buffer, count, cancelatlionToken) => destination.WriteAsync(buffer, 0, count, cancellationToken),
                    bufferSize,
                    cancellationToken
                );
        }
    }
}
