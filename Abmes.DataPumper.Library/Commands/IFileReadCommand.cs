using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public interface IFileReadCommand
    {
        int ByteCountToRead { get; set; }

        void Execute();
        Task ExecuteAsync(CancellationToken cancellationToken);

        int ResultDataLength { get; }
        void CopyResultDataTo(byte[] buffer, int offset);
        Task CopyResultDataToAsync(byte[] buffer, int offset, CancellationToken cancellationToken);
    }
}