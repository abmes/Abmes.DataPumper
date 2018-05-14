using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Abmes.DataPumper.Library;
using Abmes.DataPumper.Library.Commands;
using Abmes.DataPumper.Library.Queries;

namespace Abmes.DataPumper.WebApi
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();

            services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();
            services.AddScoped<IDataPumperDbConnection, DataPumperDbConnection>();
            services.AddTransient<IDbFileService, DbFileService>();
            services.AddTransient<ISqlConnectionStringProvider, SqlConnectionStringProvider>();
            services.AddTransient<IExporter, OracleExporter>();
            services.AddTransient<IImporter, OracleImporter>();
            services.AddTransient<IFileOpenCommand, OracleFileOpenCommand>();
            services.AddTransient<IFileCloseCommand, OracleFileCloseCommand>();
            services.AddTransient<IFileReadCommand, OracleFileReadCommand>();
            services.AddTransient<IFileWriteCommand, OracleFileWriteCommand>();
            services.AddTransient<IFileDeleteCommand, OracleFileDeleteCommand>();
            services.AddTransient<IFileExistsQuery, OracleFileExistsQuery>();
            services.AddTransient<IGetFilesQuery, AmazonOracleGetFilesQuery>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();
            loggerFactory.AddAWSProvider(this.Configuration.GetAWSLoggingConfigSection());

            app.UseMiddleware<ErrorHandlingMiddleware>();

            app.UseMvc();
        }
    }
}
