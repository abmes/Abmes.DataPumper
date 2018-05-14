using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public interface IFileOpenCommand
    {
        FileAccessMode Mode { get; set; }
        string FileName { get; set; }
        string DirectoryName { get; set; }

        void Execute();
        Task ExecuteAsync(CancellationToken cancellationToken);
    }
}