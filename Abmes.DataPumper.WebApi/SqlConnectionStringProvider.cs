using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Primitives;
using Abmes.DataPumper.Library;
using System;
using System.Diagnostics;

namespace Abmes.DataPumper
{
    public class SqlConnectionStringProvider : ISqlConnectionStringProvider
    {
        private readonly IServiceProvider _serviceProvider;

        public SqlConnectionStringProvider(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public string GetConnectionString()
        {
            var httpContextAccessor = _serviceProvider.GetService<IHttpContextAccessor>();
            Debug.Assert(httpContextAccessor != null);

            var httpContext = httpContextAccessor.HttpContext;

            // tezi ne triabva da idvat ot querystring a ot headers. ili pone usera i parolata

            StringValues dataSource;
            if (!httpContext.Request.Query.TryGetValue("DataSource", out dataSource))
            {
                throw new Exception("No DataSource param found in query string");
            }

            StringValues dataPumperUserId;
            if (!httpContext.Request.Headers.TryGetValue("DataPumperUserId", out dataPumperUserId))
            {
                throw new Exception("No DataPumperUserId header found");
            }

            StringValues dataPumperUserPassword;
            if (!httpContext.Request.Headers.TryGetValue("DataPumperUserPassword", out dataPumperUserPassword))
            {
                throw new Exception("No DataPumperUserPassword header found");
            }

            return $"Data Source={dataSource}; User Id={dataPumperUserId}; Password={dataPumperUserPassword};Pooling=false";
        }
    }
}
