using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Abmes.DataPumper.Library;
using Microsoft.Net.Http.Headers;
using System.IO.Compression;
using Abmes.DataPumper.WebApi.Utils;
using Microsoft.AspNetCore.Http;
using System.Net.Http;
using System.Threading;
using MimeKit;
using System.Net.Mime;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace Abmes.DataPumper.WebApi.Controllers
{
    [Route("api/[controller]")]
    public class DataPumperController : Controller
    {
        private readonly IExporter _exporter;
        private readonly IImporter _importer;
        private readonly IDbFileService _dbFileService;

        public DataPumperController(IDbFileService dbFileService, IExporter exporter, IImporter importer)
        {
            _dbFileService = dbFileService;
            _exporter = exporter;
            _importer = importer;
        }

        [Route("GetFiles")]
        [HttpGet]
        public async Task<IEnumerable<object>> GetFiles(CancellationToken cancellationToken, [FromQuery] string directoryName)
        {
            return await _dbFileService.GetFilesAsync(directoryName, cancellationToken);
        }

        [Route("FileExists/{fileName}")]
        [HttpGet]
        public async Task<bool> FileExists(CancellationToken cancellationToken, string fileName, [FromQuery] string directoryName)
        {
            return await _dbFileService.FileExistsAsync(fileName, directoryName, cancellationToken);
        }

        [Route("DeleteFile/{fileName}")]
        [HttpPost]
        public async Task DeleteFile(CancellationToken cancellationToken, string fileName, [FromQuery] string directoryName)
        {
            await _dbFileService.DeleteFileAsync(fileName, directoryName, cancellationToken);
        }

        // GET api/datapumper/GetFile/filename.dmp?directoryName=DATA_PUMP_DIR
        [Route("GetFile/{fileName}")]
        [HttpGet]
        public IActionResult GetFile(CancellationToken cancellationToken, string fileName, [FromQuery] string directoryName)
        {
            return new FileCallbackResult(
                new MediaTypeHeaderValue(GetMimeType(fileName)),
                async (outputStream, _) =>
                {
                    using (var stream = _dbFileService.GetFileReadStream(fileName, directoryName))
                    {
                        await stream.CopyToParallelAsync(outputStream, 32000 * 10, cancellationToken);
                    }
                });
        }

        private static string GetMimeType(string fileName)
        {
            return MimeTypes.GetMimeType(fileName) ?? MediaTypeNames.Application.Octet;
        }

        // GET api/datapumper/GetZipFile/filename.zip?sourceFileName=somefile.dmp&directoryName=DATA_PUMP_DIR
        [Route("GetZipFile/{zipFileName}")]
        [HttpGet]
        public IActionResult GetZipFile(CancellationToken cancellationToken, string zipFileName, [FromQuery] string sourceFileName, [FromQuery] string directoryName)
        {
            return new FileCallbackResult(
                new MediaTypeHeaderValue(GetMimeType(zipFileName)),
                async (outputStream, _) =>
                {
                    using (var zipArchive = new ZipArchive(new WriteOnlyStreamWrapper(outputStream), ZipArchiveMode.Create))
                    {
                        var zipEntry = zipArchive.CreateEntry(sourceFileName, CompressionLevel.Optimal);
                        using (var zipStream = zipEntry.Open())
                        {
                            using (var stream = _dbFileService.GetFileReadStream(sourceFileName, directoryName))
                            {
                                await stream.CopyToParallelAsync(zipStream, 32000 * 10, cancellationToken);
                            }
                        }
                    }
                });
        }

        // GET api/datapumper/GetGzipFile/filename.gzip?sourceFileName=somefile.dmp&directoryName=DATA_PUMP_DIR
        [Route("GetGzipFile/{gzipFileName}")]
        [HttpGet]
        public IActionResult GetGzipFile(CancellationToken cancellationToken, string gzipFileName, [FromQuery] string sourceFileName, [FromQuery] string directoryName)
        {
            return new FileCallbackResult(
                new MediaTypeHeaderValue(GetMimeType(gzipFileName)),
                async (outputStream, _) =>
                {
                    using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Optimal))
                    {
                        using (var stream = _dbFileService.GetFileReadStream(sourceFileName, directoryName))
                        {
                            await stream.CopyToParallelAsync(gzipStream, 32000 * 10, cancellationToken);
                        }
                    }
                });
        }

        [Route("PutFile")]
        [HttpPut]
        public async Task PutFile(CancellationToken cancellationToken, IFormFile file, [FromQuery] string directoryName)
        {
            if (file == null)
            {
                throw new Exception("File is null");
            }

            if (file.Length == 0)
            {
                throw new Exception("File is empty");
            }

            using (var formFileStream = file.OpenReadStream())
            {
                using (var fileStream = _dbFileService.GetFileWriteStream(file.FileName, directoryName))
                {
                    await formFileStream.CopyToParallelAsync(fileStream, 32000 * 10, cancellationToken);
                }
            }
        }

        [Route("PutFile/{fileName}")]
        [HttpPut]
        public async Task PutFile(CancellationToken cancellationToken, string fileName, [FromQuery] string sourceUrl, [FromQuery] string directoryName)
        {
            using (var httpClient = new HttpClient())
            {
                using (var sourceStream = await httpClient.GetStreamAsync(sourceUrl))
                {
                    using (var fileStream = _dbFileService.GetFileWriteStream(fileName, directoryName))
                    {
                        await sourceStream.CopyToParallelAsync(fileStream, 32000 * 10, cancellationToken);
                    }
                }
            }
        }

        [Route("PutUnzipFile/{fileName}")]
        [HttpPut]
        public async Task PutUnzipFile(CancellationToken cancellationToken, string fileName, [FromQuery] string sourceUrl, [FromQuery] string directoryName)
        {
            using (var httpClient = new HttpClient())
            {
                using (var sourceStream = await httpClient.GetStreamAsync(sourceUrl))
                {
                    using (var reader = SharpCompress.Readers.ReaderFactory.Open(sourceStream))
                    {
                        while (reader.MoveToNextEntry())
                        {
                            if ((string.IsNullOrEmpty(fileName)) || (string.Equals(reader.Entry.Key, fileName, StringComparison.InvariantCultureIgnoreCase)))
                            {
                                using (var entryStream = reader.OpenEntryStream())
                                {
                                    using (var fileStream = _dbFileService.GetFileWriteStream(reader.Entry.Key, directoryName))
                                    {
                                        await entryStream.CopyToParallelAsync(fileStream, 32000 * 10, cancellationToken);
                                    }
                                }

                                if (!string.IsNullOrEmpty(fileName))
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        [Route("PutUngzipFile/{fileName}")]
        [HttpPut]
        public async Task PutUngzipFile(CancellationToken cancellationToken, string fileName, [FromQuery] string sourceUrl, [FromQuery] string directoryName)
        {
            using (var httpClient = new HttpClient())
            {
                using (var sourceStream = await httpClient.GetStreamAsync(sourceUrl))
                {
                    using (var gzipStream = new GZipStream(sourceStream, CompressionMode.Decompress))
                    {
                        using (var fileStream = _dbFileService.GetFileWriteStream(fileName, directoryName))
                        {
                            await gzipStream.CopyToParallelAsync(fileStream, 32000 * 10, cancellationToken);
                        }
                    }
                }
            }
        }

        // POST api/datapumper/StartExportSchema?schemaName=DEV_BLA&dumpFileName=dev_bla.dmp&logFileName=dev_bla-export.log&directoryName=DATA_PUMP_DIR
        [Route("StartExportSchema")]
        [HttpPost]
        public async Task StartExportSchema(
            CancellationToken cancellationToken,
            [FromQuery] string schemaName,
            [FromQuery] string dumpFileName, 
            [FromQuery] string logFileName,
            [FromQuery] string directoryName,
            [FromQuery] string dumpFileSize)
        {
            await _exporter.StartExportSchemaAsync(schemaName, dumpFileName, logFileName, directoryName, dumpFileSize, cancellationToken);
        }

        // POST api/datapumper/StartImportSchema?fromSchemaName=DEV_BLA&toSchemaName=DEV_BLA&toSchemaPassword=DevBlaPass123&dumpFileName=dev_bla.dmp&logFileName=dev_bla-import.log&directoryName=DATA_PUMP_DIR
        [Route("StartImportSchema")]
        [HttpPost]
        public async Task StartImportSchema(
            CancellationToken cancellationToken,
            [FromQuery] string fromSchemaName,
            [FromQuery] string toSchemaName,
            [FromQuery] string toSchemaPassword,
            [FromQuery] string dumpFileName,
            [FromQuery] string logFileName,
            [FromQuery] string directoryName)
        {
            await _importer.StartImportSchemaAsync(fromSchemaName, toSchemaName, toSchemaPassword, dumpFileName, logFileName, directoryName, cancellationToken);
        }

        // GET api/datapumper/GetExportLogData?schemaName=DEV_BLA&ogFileName=dev_bla-export.log&directoryName=DATA_PUMP_DIR
        [Route("GetExportLogData")]
        [HttpGet]
        public async Task<ExportLogData> GetExportLogData(
            CancellationToken cancellationToken,
            [FromQuery] string schemaName,
            [FromQuery] string logFileName,
            [FromQuery] string directoryName)
        {
            return await _exporter.GetExportLogDataAsync(schemaName, logFileName, directoryName, cancellationToken);
        }
    }
}
