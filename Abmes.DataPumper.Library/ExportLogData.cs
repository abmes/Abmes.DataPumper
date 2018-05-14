using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class ExportLogData
    {
        public string DbName { get; }
        public bool Started { get; }
        public DateTimeOffset? StartTime { get; }
        public bool Finished { get; }
        public DateTimeOffset? FinishTime { get; }
        public bool HasErrors { get; }

        public ExportLogData(string dbName, bool started, DateTimeOffset? startTime, bool finished, DateTimeOffset? finishTime, bool hasErrors)
        {
            DbName = dbName;
            Started = started;
            StartTime = startTime;
            Finished = finished;
            FinishTime = finishTime;
            HasErrors = hasErrors;
        }
    }
}
