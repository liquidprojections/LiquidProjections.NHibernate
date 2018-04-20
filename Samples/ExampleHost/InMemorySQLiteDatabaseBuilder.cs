using System;
using System.Configuration;
using System.Data.SQLite;
using System.IO;
using System.Reflection;
using FluentNHibernate.Automapping;
using FluentNHibernate.Automapping.Steps;
using FluentNHibernate.Cfg;
using FluentNHibernate.Cfg.Db;
using LiquidProjections.ExampleHost.Projections;
using NHibernate;
using NHibernate.Tool.hbm2ddl;

namespace LiquidProjections.ExampleHost
{
    internal sealed class InMemorySqLiteDatabaseBuilder
    {
        public InMemorySqLiteDatabase Build()
        {
            string databasePath = Path.Combine(Environment.CurrentDirectory, "projections.db");

            var connectionStringSettings = new ConnectionStringSettings(
                "projections", $"FullUri=file:{databasePath}?cache=shared", "sqlite");

            var connection = new SQLiteConnection(connectionStringSettings.ConnectionString);
            connection.Open();

            ISessionFactory sessionFactory = Fluently.Configure()
                .Database(SQLiteConfiguration.Standard.ConnectionString(connectionStringSettings.ConnectionString))
                .Mappings(configuration => configuration.AutoMappings.Add(AutoMap
                    .AssemblyOf<DocumentCountProjection>(new FluentConfiguration())
                    .UseOverridesFromAssembly(Assembly.GetExecutingAssembly())
                    ))
                .ExposeConfiguration(configuration => new SchemaExport(configuration)
                    .Execute(useStdOut: true, execute: true, justDrop: false))
                .BuildSessionFactory();

            return new InMemorySqLiteDatabase(connection, sessionFactory);
        }
    }

    internal class FluentConfiguration : DefaultAutomappingConfiguration
    {
        public override bool ShouldMap(Type type)
        {
            return typeof(IPersistable).IsAssignableFrom(type);
        }

        public override bool IsComponent(Type type)
        {
            return type == typeof(ValidityPeriod);
        }
    }

    internal sealed class InMemorySqLiteDatabase : IDisposable
    {
        private readonly SQLiteConnection connection;

        public InMemorySqLiteDatabase(SQLiteConnection connection, ISessionFactory sessionFactory)
        {
            this.connection = connection;
            SessionFactory = sessionFactory;
        }

        public ISessionFactory SessionFactory { get; }

        public void Dispose()
        {
            connection?.Dispose();
        }
    }
}