using System;
using Microsoft.Owin.Hosting;
using NHibernate;
using TinyIoC;

namespace LiquidProjections.ExampleHost
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var eventStore = new JsonFileEventStore("ExampleEvents.zip", 100);

            var store = new InMemorySqLiteDatabaseBuilder().Build();

            var dispatcher = new Dispatcher(eventStore.Subscribe);

            var projector = new CountsProjector(dispatcher, store.SessionFactory.OpenSession);

            var startOptions = new StartOptions($"http://localhost:9000");
            using (WebApp.Start(startOptions, builder => builder.UseStatisticsApi(() => store.SessionFactory.OpenSession())))
            {
                projector.Start();

                Console.WriteLine($"HTTP endpoint available at http://localhost:9000/api/Statistics/CountsPerState");
                Console.ReadLine();
            }
        }
    }
}