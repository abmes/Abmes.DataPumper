using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library.Commands
{
    public interface IFileCloseCommand
    {
        void Execute();
        Task ExecuteAsync(CancellationToken cancellationToken);
    }
}