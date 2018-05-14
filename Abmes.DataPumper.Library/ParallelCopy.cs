using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public static class ParallelCopy
    {
        public static async Task CopyAsync(Func<byte[], CancellationToken, Task<int>> copyReadTask, Func<byte[], int, CancellationToken, Task> copyWriteTask, Int32 bufferSize, CancellationToken cancellationToken)
        {
            Contract.Requires(copyReadTask != null);
            Contract.Requires(copyWriteTask != null);
            Contract.Requires(bufferSize > 0);

            byte[][] buffers = { ArrayPool<byte>.Shared.Rent(bufferSize), ArrayPool<byte>.Shared.Rent(bufferSize) };

            try
            {
                var bufferIndex = 0;
                var writeTask = Task.CompletedTask;
                while (true)
                {
                    var readTask = copyReadTask(buffers[bufferIndex], cancellationToken);

                    await Task.WhenAll(readTask, writeTask);

                    int bytesRead = readTask.Result;

                    if (bytesRead == 0)
                    {
                        break;
                    }

                    writeTask = copyWriteTask(buffers[bufferIndex], bytesRead, cancellationToken);

                    bufferIndex = 1 - bufferIndex;
                }
            }
            finally
            {
                foreach (var b in buffers)
                    ArrayPool<byte>.Shared.Return(b, clearArray: true);
            }
        }
    }
}
