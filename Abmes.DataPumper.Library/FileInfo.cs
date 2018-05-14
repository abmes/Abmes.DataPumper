using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class FileInfo
    {
        public string FileName { get; }
        public long FileSize { get; }
        public DateTime LastModifiedDateTime { get; }

        public FileInfo(string fileName, long fileSize, DateTime lastModifiedDateTime)
        {
            FileName = fileName;
            FileSize = fileSize;
            LastModifiedDateTime = lastModifiedDateTime;
        }
    }
}
