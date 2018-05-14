using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public interface IFileDeleteCommand
    {
        string FileName { get; set; }
        string DirectoryName { get; set; }

        void Execute();
        Task ExecuteAsync(CancellationToken cancellationToken);
    }
}