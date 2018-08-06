using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Abmes.DataPumper.Library
{
    public class OracleExporter : IExporter
    {
        private readonly IDataPumperDbConnection _dbConnection;
        private readonly IDbFileService _dbFileService;

        public OracleExporter(
            IDataPumperDbConnection dbConnection,
            IDbFileService dbFileService)
        {
            _dbConnection = dbConnection;
            _dbFileService = dbFileService;
        }

        private string OraclizeFileName(string fileName)
            =>  fileName?.Replace("~partno~", "%U");

        public async Task StartExportSchemaAsync(string schemaName, string dumpFileName, string logFileName, string directoryName, string dumpFileSize, CancellationToken cancellationToken)
        {
            var sql =
                "begin" + Environment.NewLine +
                "  DataPumperUtils.StartExportSchema(:SCHEMA_NAME, :DUMP_FILE_NAME, :LOG_FILE_NAME, :DIRECTORY_NAME, :DUMP_FILE_SIZE);" + Environment.NewLine +
                "end;";

            using (var command = new OracleCommand(sql, _dbConnection.OracleConnection))
            {
                command.Parameters.Add(new OracleParameter("SCHEMA_NAME", schemaName));
                command.Parameters.Add(new OracleParameter("DUMP_FILE_NAME", OraclizeFileName(dumpFileName)));
                command.Parameters.Add(new OracleParameter("LOG_FILE_NAME", logFileName));
                command.Parameters.Add(new OracleParameter("DIRECTORY_NAME", directoryName));
                command.Parameters.Add(new OracleParameter("DUMP_FILE_SIZE", dumpFileSize));
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        private IEnumerable<string> ReadAllLines(StreamReader reader)
        {
            while (true)
            {
                var line = reader.ReadLine();

                if (line == null)
                {
                    yield break;
                }

                yield return line;
            }
        }

        private TimeSpan ParseOracleElapsed(string elapsed)
        {
            return TimeSpan.ParseExact(elapsed.Replace(" ", ":") + ".000", "G", CultureInfo.InvariantCulture);
        }

        public async Task<ExportLogData> GetExportLogDataAsync(string schemaName, string logFileName, string directoryName, CancellationToken cancellationToken)
        {
            var logFileLines = GetLogFileLines(logFileName, directoryName).ToList();  // ToList() fixes some sequence empty issues

            if (!logFileLines.Any())
            {
                return new ExportLogData(schemaName, false, null, false, null, false);
            }

            const string jobStartingPrefix = ";;; Job starting at ";
            var firstLine = logFileLines.First();
            var startTime =
                firstLine.StartsWith(jobStartingPrefix) ?
                DateTimeOffset.ParseExact(firstLine.Substring(jobStartingPrefix.Length), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) :
                (DateTimeOffset?)null;

            const string jobElapsed = "elapsed ";
            var lastLine = logFileLines.Last();
            var elapsedTime =
                lastLine.Contains(jobElapsed) ?
                ParseOracleElapsed(lastLine.Split(new[] { jobElapsed }, StringSplitOptions.None).Last()) :
                (TimeSpan?)null;

            var finishTime = startTime + elapsedTime;

            var hasErrors = finishTime.HasValue && !lastLine.Contains("successfully completed");

            var result = new ExportLogData(schemaName, startTime.HasValue, startTime, finishTime.HasValue, finishTime, hasErrors);

            return await Task.FromResult(result);
        }

        private IEnumerable<string> GetLogFileLines(string logFileName, string directoryName)
        {
            if (!_dbFileService.FileExistsAsync(logFileName, directoryName, CancellationToken.None).Result)
            {
                return Enumerable.Empty<string>();
            }

            var logFileStream = _dbFileService.GetFileReadStream(logFileName, directoryName);
            var reader = new StreamReader(logFileStream);
            return ReadAllLines(reader);
        }
    }
}
