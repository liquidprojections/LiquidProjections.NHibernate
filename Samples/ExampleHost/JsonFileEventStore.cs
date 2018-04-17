using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization.Formatters;
using System.Threading.Tasks;
using LiquidProjections.Abstractions;
using Newtonsoft.Json;
using NHibernate.Hql.Ast.ANTLR.Tree;

namespace LiquidProjections.ExampleHost
{
    public class JsonFileEventStore : IDisposable
    {
        private readonly int pageSize;
        private ZipArchive zip;
        private readonly Queue<ZipArchiveEntry> entryQueue;
        private StreamReader currentReader = null;
        private static long lastCheckpoint = 0;

        public JsonFileEventStore(string filePath, int pageSize)
        {
            this.pageSize = pageSize;
            zip = ZipFile.Open(filePath, ZipArchiveMode.Read);
            entryQueue = new Queue<ZipArchiveEntry>(zip.Entries.Where(e => e.Name.EndsWith(".json")));
        }

        public IDisposable Subscribe(long? lastProcessedCheckpoint, Subscriber subscriber, string subscriptionId)
        {
            var subscription = new Subscription(lastProcessedCheckpoint ?? 0, subscriber);
            
            Task.Run(async () =>
            {
                Task<Transaction[]> loader = LoadNextPageAsync();
                Transaction[] transactions = await loader;

                while (transactions.Length > 0)
                {
                    // Start loading the next page on a separate thread while we have the subscriber handle the previous transactions.
                    loader = LoadNextPageAsync();

                    await subscription.Send(transactions);

                    transactions = await loader;
                }
            });

            return subscription;
        }

        private Task<Transaction[]> LoadNextPageAsync()
        {
            return Task.Run(() =>
            {
                var transactions = new List<Transaction>();

                Transaction transaction = null;

                string json;

                do
                {
                    json = CurrentReader.ReadLine();
                    if (json != null)
                    {
                        object @event = JsonConvert.DeserializeObject(json, new JsonSerializerSettings
                        {
                            TypeNameHandling = TypeNameHandling.All,
                            TypeNameAssemblyFormat = FormatterAssemblyStyle.Full
                        });

                        string streamId = ExtractStreamIdFrom(@event);
                        if (transaction != null && transaction.StreamId != streamId)
                        {
                            transaction = null;
                        }

                        if (transaction == null)
                        {
                            transaction = new Transaction
                            {
                                Id = Guid.NewGuid().ToString(),
                                TimeStampUtc = DateTime.UtcNow,
                                StreamId = streamId,
                                Checkpoint = lastCheckpoint++
                            };
                            
                            transactions.Add(transaction);
                        }

                        transaction.Events.Add(new EventEnvelope
                        {
                            Body = @event
                        });
                    }
                }
                while ((json != null) && (transactions.Count < pageSize));

                Console.WriteLine(
                    $"Loaded page of {transactions.Count} transactions " + 
                    $"(checkpoint {transactions.First().Checkpoint}-{transactions.Last().Checkpoint}) " +
                    $"with {transactions.Sum(t => t.Events.Count)} events");
                
                return transactions.ToArray();
            });
        }

        private string ExtractStreamIdFrom(object @event)
        {
            PropertyInfo property =
                @event.GetType().GetProperty("Code") ??
                @event.GetType().GetProperty("Number") ??
                @event.GetType().GetProperty("DocumentNumber");

            return property.GetValue(@event, null).ToString();
        }

        private StreamReader CurrentReader => 
            currentReader ?? (currentReader = new StreamReader(entryQueue.Dequeue().Open()));

        public void Dispose()
        {
            zip.Dispose();
            zip = null;
        }

        internal class Subscription : IDisposable
        {
            private readonly long lastProcessedCheckpoint;
            private readonly Subscriber subscriber;
            private bool disposed;

            public Subscription(long lastProcessedCheckpoint, Subscriber subscriber)
            {
                this.lastProcessedCheckpoint = lastProcessedCheckpoint;
                this.subscriber = subscriber;
            }

            public async Task Send(IEnumerable<Transaction> transactions)
            {
                if (!disposed)
                {
                    Transaction[] readOnlyList = transactions.Where(t => t.Checkpoint > lastProcessedCheckpoint).ToArray();
                    if (readOnlyList.Length > 0)
                    {
                        await subscriber.HandleTransactions(readOnlyList, new SubscriptionInfo
                        {
                            Id = "subscription",
                            Subscription = this
                        });
                    }
                }
                else
                {
                    throw new ObjectDisposedException("");
                }
            }

            public void Dispose()
            {
                disposed = true;
            }
        }
    }
}