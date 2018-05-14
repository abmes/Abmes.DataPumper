using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public interface IFileWriteCommand
    {
        void CopyDataFrom(byte[] buffer, int offset, int count);
        Task CopyDataFromAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken);
        void Execute();
        Task ExecuteAsync(CancellationToken cancellationToken);
    }
}