using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public interface ISqlConnectionStringProvider
    {
        string GetConnectionString();
    }
}
