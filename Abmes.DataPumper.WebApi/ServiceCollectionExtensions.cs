using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Abmes.DataPumper.WebApi
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddFactoryFunc<TService>(this IServiceCollection services) where TService : class
        {
            return services.AddTransient<Func<TService>>(ctx => () => ctx.GetService<TService>());
        }
    }
}
