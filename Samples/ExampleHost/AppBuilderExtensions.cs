using System;
using System.Collections.Generic;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using NHibernate;
using Owin;
using TinyIoC;

namespace LiquidProjections.ExampleHost
{
    internal static class AppBuilderExtensions
    {
        internal static IAppBuilder UseStatisticsApi(this IAppBuilder app, Func<ISession> sessionFactory)
        {            
            var container = TinyIoCContainer.Current;
            container.Register<Func<ISession>>(sessionFactory);
            HttpConfiguration httpConfiguration = BuildHttpConfiguration(container);

            app.Map("/api", a => a.UseWebApi(httpConfiguration));

            return app;
        }

        private static HttpConfiguration BuildHttpConfiguration(TinyIoCContainer container)
        {
            var configuration = new HttpConfiguration
            {
                DependencyResolver = new TinyIocWebApiDependencyResolver(container),
                IncludeErrorDetailPolicy = IncludeErrorDetailPolicy.Always
            };

            configuration.Services.Replace(typeof(IHttpControllerTypeResolver), new ControllerTypeResolver());
            configuration.MapHttpAttributeRoutes();

            return configuration;
        }

        private class ControllerTypeResolver : IHttpControllerTypeResolver
        {
            public ICollection<Type> GetControllerTypes(IAssembliesResolver assembliesResolver)
            {
                return new List<Type>
                {
                    typeof(StatisticsController)
                };
            }
        }
    }
}